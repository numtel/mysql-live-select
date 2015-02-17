var EventEmitter = require('events').EventEmitter;
var _            = require('lodash');

var murmurHash    = require('../dist/murmurhash3_gc');
var querySequence = require('./querySequence');

var queue = [], queueBusy = false;

class RowTrigger extends EventEmitter {
  constructor(parent, table, payloadColumns) {
    this.table          = table;
    this.payloadColumns = payloadColumns;
    this.ready          = false;
    this.stopped        = false;

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

    var triggerHash = murmurHash(JSON.stringify(this.payloadColumns));
    var triggerName = `${channel}_${table}_${triggerHash}`;

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

  stop(callback) {
    if(this.stopped) {
      return callback();
    }

    // Remove the CREATE sql from the queue if it exists
    for(var i in queue) {
      if(queue[i].instance === this) {
        queue = queue.splice(i, 1);
        break;
      }
    }

    // Drop the trigger and function if they exist
    var sql = [
      `DROP TRIGGER IF EXISTS "${this.triggerName}" ON ${this.table}`,
      `DROP FUNCTION IF EXISTS "${this.triggerName}()"`
    ];

    querySequence(this.client, sql, (error, result) => {
      this.stopped = true;
      callback(error, result);
    });
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
