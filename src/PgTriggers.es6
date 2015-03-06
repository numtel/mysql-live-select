var _            = require('lodash');
var pg           = require('pg');
var EventEmitter = require('events').EventEmitter;
var murmurHash    = require('murmurhash-js').murmur3;

var LiveSelect    = require('./LiveSelect');
var querySequence = require('./querySequence');

// Number of milliseconds between refreshing result sets
const THROTTLE_INTERVAL = 100;

class PgTriggers extends EventEmitter {
	constructor(connectionString, channel, hashTable) {
		this.connectionString  = connectionString;
		this.channel           = channel;
		this.triggerTables     = {};
		this.notifyClient      = null;
		this.notifyClientDone  = null;
		this.cachedQueryTables = {};
		this.resultCache       = {};
		this.waitingToUpdate   = [];
		this.updateInterval    = null;

		this.setMaxListeners(0); // Allow unlimited listeners

		this.init = new Promise((resolve, reject) => {
			// Reserve one client to listen for notifications
			this.getClient((error, client, done) => {
				if(error) return this.emit('error', error);

				this.notifyClient     = client;
				this.notifyClientDone = done;

				querySequence(client, [
					`LISTEN "${channel}"`
				]).then(resolve, error => { this.emit('error', error); reject(error) });

				client.on('notification', info => {
					if(info.channel === channel && info.payload in this.triggerTables){
						this.waitingToUpdate = _.union(
							this.waitingToUpdate,
							this.triggerTables[info.payload].updateFunctions);
					}
				});

				// Initialize throttled updater
				// TODO also update when at a threshold waitingToUpdate length
				this.updateInterval =
					setInterval(this.refresh.bind(this), THROTTLE_INTERVAL);

			});
		})
	}

	getClient(cb) {
		pg.connect(this.connectionString, cb);
	}

	select(query, params) {
		var newSelect = new LiveSelect(this, query, params);
		newSelect.init.catch(error => this.emit('error', error));
		return newSelect
	}

	async registerQueryTriggers(query, updateFunction) {
		var { channel, triggerTables } = this;

		var tables = await this.getQueryTables(query);

		for(var i = 0; i<tables.length; i++){
			let table = tables[i]

			if(!(table in triggerTables)) {
				// Create the trigger for this table on this channel
				var triggerName = `${channel}_${table}`;

				triggerTables[table] = querySequence(this, [
					`CREATE OR REPLACE FUNCTION ${triggerName}() RETURNS trigger AS $$
						BEGIN
							NOTIFY "${channel}", '${table}';
							RETURN NULL;
						END;
					$$ LANGUAGE plpgsql`,
					`DROP TRIGGER IF EXISTS "${triggerName}"
						ON "${table}"`,
					`CREATE TRIGGER "${triggerName}"
						AFTER INSERT OR UPDATE OR DELETE ON "${table}"
						EXECUTE PROCEDURE ${triggerName}()`
				]);

				triggerTables[table].updateFunctions = [ updateFunction ];

			}else{
				if(triggerTables[table].updateFunctions.indexOf(updateFunction) === -1){
					triggerTables[table].updateFunctions.push(updateFunction);
				}
			}

			await triggerTables[table];
		}

		return tables;
	}

	refresh() {
		var updateCount = this.waitingToUpdate.length;
		if(updateCount === 0) return;

		this.waitingToUpdate.splice(0, updateCount).map(updateFunction => {
			var cache = this.resultCache[updateFunction];
			var curHashes, oldHashes, newHashes, addedRows;
			oldHashes = cache.data.map(row => row._hash);

			querySequence.noTx(this, [ [ `
				WITH
					res AS (${cache.query}),
					data AS (
						SELECT
							MD5(CAST(ROW_TO_JSON(res.*) AS TEXT)) AS _hash,
							ROW_NUMBER() OVER () AS _index,
							res.*
						FROM res),
					data2 AS (
						SELECT
							1 AS _added,
							data.*
						FROM data
						WHERE _hash NOT IN ('${oldHashes.join("','")}'))
				SELECT
					data2.*,
					data._hash AS _hash
				FROM data
				LEFT JOIN data2
					ON (data._index = data2._index)
			`, cache.params ] ]).then(result => {
				curHashes = result[0].rows.map(row => row._hash);
				newHashes = curHashes.filter(
					hash => oldHashes.indexOf(hash) === -1);

				var curHashes2 = curHashes.slice();
				addedRows = result[0].rows
					.filter(row => row._added === 1)
					.map((row, index) => {
						row._index = curHashes2.indexOf(row._hash) + 1;
						delete row._added;

						// Clear this hash so that duplicate hashes can move forward
						curHashes2[row._index - 1] = undefined;

						return row;
					})

				var movedHashes = curHashes.map((hash, newIndex) => {
					var oldIndex = oldHashes.indexOf(hash);
					if(oldIndex !== -1 &&
							oldIndex !== newIndex &&
							curHashes[oldIndex] !== hash) {
						return {
							old_index: oldIndex + 1,
							new_index: newIndex + 1,
							_hash: hash
						}
					}
				}).filter(moved => moved !== undefined);

				var removedHashes = oldHashes
					.map((_hash, index) => { return { _hash, _index: index + 1 } })
					.filter(removed =>
						curHashes[removed._index - 1] !== removed._hash &&
						movedHashes.filter(moved =>
							moved.new_index === removed._index).length === 0);

				// Add rows that have already existing hash but in new places
				var copiedHashes = curHashes.map((hash, index) => {
					var oldHashIndex = oldHashes.indexOf(hash);
					if(oldHashIndex !== -1 &&
							oldHashes[index] !== hash &&
							movedHashes.filter(moved =>
								moved.new_index - 1 === index).length === 0 &&
							addedRows.filter(added =>
								added._index - 1 === index).length === 0){
						return {
							new_index: index + 1,
							orig_index: oldHashIndex + 1
						}
					}
				}).filter(copied => copied !== undefined);

				var diff = {
					removed: removedHashes.length !== 0 ? removedHashes : null,
					moved: movedHashes.length !== 0 ? movedHashes: null,
					copied: copiedHashes.length !== 0 ? copiedHashes: null,
					added: addedRows.length !== 0 ? addedRows : null
				};

				if(diff.added === null &&
						diff.moved === null &&
						diff.copied === null &&
						diff.removed === null) return;

				var rows = cache.data =
					this.calcUpdatedResultCache(cache.data, diff);

				this.emit(updateFunction, diff, rows);
			}, error => this.emit('error', error));

		});
	}

	calcUpdatedResultCache(oldResults, diff) {
		var newResults = oldResults.slice();

		diff.removed !== null && diff.removed
			.forEach(removed => newResults[removed._index - 1] = undefined);

		// Deallocate first to ensure no overwrites
		diff.moved !== null && diff.moved.forEach(moved => {
			newResults[moved.old_index - 1] = undefined;
		});

		diff.copied !== null && diff.copied.forEach(copied => {
			var copyRow = _.clone(oldResults[copied.orig_index - 1]);
			if(!copyRow){
				// TODO why do some copied rows not exist in the old data?
				console.log(copied, oldResults.length)
			}
			copyRow._index = copied.new_index;
			newResults[copied.new_index - 1] = copyRow;
		});

		diff.moved !== null && diff.moved.forEach(moved => {
			var movingRow = oldResults[moved.old_index - 1];
			movingRow._index = moved.new_index;
			newResults[moved.new_index - 1] = movingRow;
		});

		diff.added !== null && diff.added
			.forEach(added => newResults[added._index - 1] = added);

		return newResults.filter(row => row !== undefined)
	}

	/**
	 * Retrieve the tables used in a query
	 * @param  String query May contain placeholders as they will be nullified
	 * @return Promise
	 */
	getQueryTables(query) {
		return new Promise((resolve, reject) => {
			var queryHash = murmurHash(query);

			// If this query was cached before, reuse it
			if(this.cachedQueryTables[queryHash]) {
				return resolve(this.cachedQueryTables[queryHash]);
			}

			// Replace all parameter values with NULL
			var tmpQuery = query.replace(/\$\d/g, 'NULL');
			var tmpName  = `tmp_view_${queryHash}`;

			querySequence(this, [
				`CREATE OR REPLACE TEMP VIEW ${tmpName} AS (${tmpQuery})`,
				[`SELECT DISTINCT vc.table_name
					FROM information_schema.view_column_usage vc
					WHERE view_name = $1`, [ tmpName ] ],
			]).then(result => {
				var tables = result[1].rows.map(row => row.table_name);
				this.cachedQueryTables[queryHash] = tables;
				resolve(tables);
			}, reject);
		})
	}

	/**
	 * Drop all active triggers and close notification client
	 * @param  Function callback Optional (error, result)
	 * @return Promise
	 */
	cleanup(callback) {
		var { triggerTables, channel } = this;

		this.notifyClientDone();
		this.removeAllListeners();
		this.updateInterval !== null && clearInterval(this.updateInterval);

		var queries = [];
		_.forOwn(triggerTables, (tablePromise, table) => {
			var triggerName = `${channel}_${table}`;

			queries.push(`DROP TRIGGER IF EXISTS ${triggerName} ON ${table}`);
			queries.push(`DROP FUNCTION IF EXISTS ${triggerName}()`);

		});

		return querySequence(this, queries, callback);
	}
}

module.exports = PgTriggers;

