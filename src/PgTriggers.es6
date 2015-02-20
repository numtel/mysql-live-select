var _            = require('lodash');
var EventEmitter = require('events').EventEmitter;

var querySequence = require('./querySequence');
var RowTrigger    = require('./RowTrigger');
var LiveSelect    = require('./LiveSelect');

class PgTriggers extends EventEmitter {
	constructor(connect, channel) {
		this.connect       = connect;
		this.channel       = channel;
		this.triggerTables = [];

		this.setMaxListeners(0); // Allow unlimited listeners

		// Reserve one client to listen for notifications
		this.getClient((error, client, done) => {
			if(error) return this.emit('error', error);

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
		if(this.client && this.done) {
			cb(null, this.client, this.done);
		}
		else {
			this.connect((error, client, done) => {
				if(error) return this.emit('error', error);

				this.client = client;
				this.done   = done;

				cb(null, this.client, this.done);
			});
		}
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
				done();
				callback(error, result);
			});
		});
	}
}

module.exports = PgTriggers;

