var EventEmitter = require('events').EventEmitter;
var _            = require('lodash');

var querySequence = require('./querySequence');

var queue = [], queueBusy = false;

class RowTrigger extends EventEmitter {
  constructor(parent, table) {
    this.table          = table;
    this.ready          = false;

    parent.on(`change:${table}`, this.forwardNotification.bind(this));

    if(parent.triggerTables.indexOf(table) === -1) {
      parent.triggerTables.push(table);

      // Create the trigger for this table on this channel
      var channel     = parent.channel;
      var triggerName = `${channel}_${table}`;

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
          if(error) return this.emit('error', error);

          this.ready = true;
          this.emit('ready');
          done();
        });
      });
    }
    else {
      // Triggers already in place
      this.ready = true;
      this.emit('ready');
    }
  }

  forwardNotification() {
    this.emit('change');
  }
}

module.exports = RowTrigger;
