var EventEmitter = require('events').EventEmitter;
var _            = require('lodash');

var querySequence = require('./querySequence');

var queue = [], queueBusy = false;

class RowTrigger extends EventEmitter {
  constructor(parent, table, payloadColumns) {
    this.table = table;
    this.payloadColumns = payloadColumns;
    this.ready = false;

    var { payloadColumnBuffer, client, channel } = parent;

    parent.on(`change:${table}`, this.forwardNotification.bind(this));

    // Merge these columns to the trigger's payload
    if(!(table in payloadColumnBuffer)) {
      payloadColumnBuffer[table] = payloadColumns.slice();
    }
    else {
      payloadColumns = payloadColumnBuffer[table] =
        _.union(payloadColumnBuffer[table], payloadColumns);
    }

    // Update the trigger for this table on this channel
    var payloadTpl = `
      SELECT
        '${table}'  AS _table,
        TG_OP       AS _op,
        ${payloadColumns.map(col => `$ROW$.${col}`).join(', ')}
      INTO row_data;
    `;
    var payloadNew = payloadTpl.replace(/\$ROW\$/g, 'NEW');
    var payloadOld = payloadTpl.replace(/\$ROW\$/g, 'OLD');
    var payloadChanged = `
      SELECT
        '${table}'  AS _table,
        TG_OP       AS _op,
        ${payloadColumns.map(col => `NEW.${col} AS new_${col}`)
          .concat(payloadColumns.map(col => `OLD.${col} AS old_${col}`))
          .join(', ')}
      INTO row_data;
    `;

    var triggerName = `${channel}_${table}`;

    queue.push({
      client,
      instance: this,
      queries: [
      `CREATE OR REPLACE FUNCTION ${triggerName}() RETURNS trigger AS $$
        DECLARE
          row_data RECORD;
        BEGIN
          IF (TG_OP = 'INSERT') THEN
            ${payloadNew}
          ELSIF (TG_OP  = 'DELETE') THEN
            ${payloadOld}
          ELSIF (TG_OP = 'UPDATE') THEN
            ${payloadChanged}
          END IF;
          PERFORM pg_notify('${channel}', row_to_json(row_data)::TEXT);
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

  }
  forwardNotification(payload) {
    this.emit('change', payload);
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
      processItem.instance.emit('ready', results);

      // Continue in queue
      queueBusy = false;
      processQueue();
    });
  }else{
    queueBusy = false;
  }
}
