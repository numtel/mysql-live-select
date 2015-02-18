var EventEmitter = require('events').EventEmitter;
var _            = require('lodash');

var querySequence = require('./querySequence');

var queue = [], queueBusy = false;

class RowTrigger extends EventEmitter {
  constructor(parent, table) {
    this.table          = table;
    this.ready          = false;

    var { client, channel } = parent;

    parent.on(`change:${table}`, this.forwardNotification.bind(this));

    if(parent.triggerTables.indexOf(table) === -1){
      parent.triggerTables.push(table);

      // Create the trigger for this table on this channel
      var triggerName = `${channel}_${table}`;

      this.triggerName = triggerName;
      this.client      = client;

      queue.push({
        client,
        instance: this,
        queries: [
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
      ]});

      processQueue();
    }else{
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

function processQueue() {
  if(queueBusy === true) return;

  queueBusy = true;
  if(queue.length > 0){
    var processItem = queue.shift();
    querySequence(processItem.client, processItem.queries, (error, results) => {
      if(error) return processItem.instance.emit('error', error);

      processItem.instance.ready = true;
      processItem.instance.emit('ready');

      // Continue in queue
      queueBusy = false;
      processQueue();
    });
  }else{
    queueBusy = false;
  }
}
