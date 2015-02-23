var _            = require('lodash');
var pg           = require('pg');
var EventEmitter = require('events').EventEmitter;
var murmurHash    = require('murmurhash-js').murmur3;

var RowCache      = require('./RowCache');
var LiveSelect    = require('./LiveSelect');
var querySequence = require('./querySequence');

class PgTriggers extends EventEmitter {
	constructor(connectionString, channel) {
		this.connectionString  = connectionString;
		this.channel           = channel;
		this.rowCache          = new RowCache;
		this.triggerTables     = {};
		this.notifyClient      = null;
		this.notifyClientDone  = null;
		this.cachedQueryTables = {};

		this.setMaxListeners(0); // Allow unlimited listeners

		// Reserve one client to listen for notifications
		this.getClient((error, client, done) => {
			if(error) return this.emit('error', error);

			this.notifyClient     = client;
			this.notifyClientDone = done;

			client.query(`LISTEN "${channel}"`, function(error, result) {
				if(error) throw error;
			});

			client.on('notification', (info) => {
				if(info.channel === channel) {
					this.emit(`change:${info.payload}`);
				}
			});
		});
	}

	getClient(cb) {
		pg.connect(this.connectionString, cb);
	}

	select(query, params) {
		return new LiveSelect(this, query, params);
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

