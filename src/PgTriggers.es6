var _            = require('lodash');
var pg           = require('pg');
var EventEmitter = require('events').EventEmitter;

var querySequence = require('./querySequence');
var RowCache      = require('./RowCache');
var RowTrigger    = require('./RowTrigger');
var LiveSelect    = require('./LiveSelect');

class PgTriggers extends EventEmitter {
	constructor(connectionString, channel) {
		this.connectionString = connectionString;
		this.channel          = channel;
		this.rowCache         = new RowCache;
		this.triggerTables    = [];
		this.notifyClient     = null;
		this.notifyClientDone = null;

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

	createTrigger(table) {
		return new RowTrigger(this, table);
	}

	select(query, params) {
		return new LiveSelect(this, query, params);
	}

	cleanup(callback) {
		var { triggerTables, channel } = this;

		var queries = [];

		this.getClient((error, client, done) => {
			if(error) return this.emit('error', error);

			_.forOwn(triggerTables, (tablePromise, table) => {
				var triggerName = `${channel}_${table}`;

				queries.push(`DROP TRIGGER IF EXISTS ${triggerName} ON ${table}`);
				queries.push(`DROP FUNCTION IF EXISTS ${triggerName}()`);
			});

			querySequence(client, queries, (error, result) => {
				this.notifyClientDone();
				callback && callback(error, result);
			});
		});
	}
}

module.exports = PgTriggers;

