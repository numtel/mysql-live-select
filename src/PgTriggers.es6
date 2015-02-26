var _            = require('lodash');
var pg           = require('pg');
var EventEmitter = require('events').EventEmitter;
var murmurHash    = require('murmurhash-js').murmur3;

var LiveSelect    = require('./LiveSelect');
var querySequence = require('./querySequence');

// Number of milliseconds between refreshing result sets
const THROTTLE_INTERVAL = 200;

class PgTriggers extends EventEmitter {
	constructor(connectionString, channel, hashTable) {
		this.connectionString  = connectionString;
		this.channel           = channel;
		this.hashTable         = hashTable || `${channel}_hashes`;
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
					`LISTEN "${channel}"`,
					`CREATE UNLOGGED TABLE IF NOT EXISTS "${this.hashTable}" (
							query_hash INTEGER PRIMARY KEY,
							row_hashes TEXT[]
						) WITH ( OIDS=FALSE )`,
					`TRUNCATE TABLE "${this.hashTable}"`
				]).then(resolve, error => { this.emit('error', error); reject(error) });

				client.on('notification', info => {
					if(info.channel === channel && info.payload in this.triggerTables){
						this.waitingToUpdate = _.union(
							this.waitingToUpdate,
							this.triggerTables[info.payload].updateFunctions);
					}
				});

				// Initialize throttled updater
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

	registerQueryTriggers(query, updateFunction) {
		var { channel, triggerTables } = this;
		return new Promise((resolve, reject) => {
			this.getQueryTables(query).then(tables => {
				tables.forEach(table => {
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
						]).catch(error => this.emit('error', error));

						triggerTables[table].updateFunctions = [];
					}

					triggerTables[table].updateFunctions.push(updateFunction);
				});
				resolve(tables);
			}, reject);
		});
	}

	refresh() {
		var updateCount = this.waitingToUpdate.length;
		if(updateCount === 0) return Promise.resolve();

		// Clear waitingToUpdate queue and prepare for selection
		var selectUpdates =
			this.waitingToUpdate.splice(0, updateCount).join('(),');

		return querySequence(this, [ `SELECT ${selectUpdates}()` ])
			.then(results => {
				_.forOwn(results[0].rows[0], (jsonData, updateFunction) => {
					try{
						var data = JSON.parse(jsonData);
					}catch(error){
						return this.emit('error', error);
					}
					this.emit(updateFunction, data);
					// TODO calc full results!
					this.resultCache[updateFunction] = data;
				})
			}, error => this.emit('error', error))
	}

	updateResultCache(updateFunction, diff) {
		
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
		this.updateInterval !== null && clearInterval(this.updateInterval);

		var queries = [];
		_.forOwn(triggerTables, (tablePromise, table) => {
			var triggerName = `${channel}_${table}`;

			queries.push(`DROP TRIGGER IF EXISTS ${triggerName} ON ${table}`);
			queries.push(`DROP FUNCTION IF EXISTS ${triggerName}()`);

			queries = queries.concat(tablePromise.updateFunctions.map(
				updateFunction => `DROP FUNCTION IF EXISTS ${updateFunction}()`));
		});

		return querySequence(this, queries, callback);
	}
}

module.exports = PgTriggers;

