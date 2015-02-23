var _            = require('lodash');
var EventEmitter = require('events').EventEmitter;

var querySequence   = require('./querySequence');

class RowTrigger extends EventEmitter {
	constructor(parent, table) {
		this.table = table;
		this.ready = false;

		var { channel, triggerTables } = parent;

		parent.on(`change:${table}`, this.forwardNotification.bind(this));

		if(!(table in triggerTables)) {
			// Create the trigger for this table on this channel
			var triggerName = `${channel}_${table}`;

			triggerTables[table] = querySequence(parent, [
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
					FOR EACH ROW EXECUTE PROCEDURE ${triggerName}()`
			]);
		}

		triggerTables[table].then(
			(result) => { this.ready = true; this.emit('ready') },
			(error) => { this.emit('error', error) }
		);
	}

	forwardNotification() {
		this.emit('change');
	}
}

module.exports = RowTrigger;
