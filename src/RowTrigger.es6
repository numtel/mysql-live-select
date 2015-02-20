var _            = require('lodash');
var EventEmitter = require('events').EventEmitter;

var querySequence   = require('./querySequence');
var triggerPromises = {};

class RowTrigger extends EventEmitter {
	constructor(parent, table) {
		this.table = table;
		this.ready = false;

		parent.on(`change:${table}`, this.forwardNotification.bind(this));

		if(!triggerPromises[table]) {
			// Create the trigger for this table on this channel
			var channel     = parent.channel;
			var triggerName = `${channel}_${table}`;

			triggerPromises[table] = new Promise((resolve, reject) => {
				parent.connect((error, client, done) => {
					if(error) return this.emit('error', error);

					var sql = [
						`CREATE OR REPLACE FUNCTION ${triggerName}() RETURNS trigger AS $$
							DECLARE
								row_data RECORD;
							BEGIN
								PERFORM pg_notify('${channel}', '${table}');
								RETURN NULL;
							END;
						$$ LANGUAGE plpgsql`,
						`DROP TRIGGER IF EXISTS "${triggerName}"
							ON "${table}"`,
						`CREATE TRIGGER "${triggerName}"
							AFTER INSERT OR UPDATE OR DELETE ON "${table}"
							FOR EACH ROW EXECUTE PROCEDURE ${triggerName}()`
					];

					querySequence(client, sql, (error, results) => {
						if(error) return reject(error);

						done();
						parent.triggerTables.push(table);
						resolve();
					});
			});
			});
		}

		triggerPromises[table]
			.then(() => {
				this.ready = true;
				this.emit('ready');
			}, (error) => {
				this.emit('error', error);
			});
	}

	forwardNotification() {
		this.emit('change');
	}
}

module.exports = RowTrigger;
