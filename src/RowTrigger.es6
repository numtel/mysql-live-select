var EventEmitter = require('events').EventEmitter;
var _            = require('lodash');

var querySequence = require('./querySequence');

class RowTrigger extends EventEmitter {
  constructor(parent, table, payloadColumns) {
    this.table = table;
    this.payloadColumns = payloadColumns;

    var { payloadColumnBuffer, conn, channel } = parent;

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
    var payloadTpl =
      `SELECT
        '${table}'  AS _table,
        TG_OP       AS _op,
        '$ROW$'     AS _which_row,
        ${payloadColumns.map(col => `$ROW$.${col}`).join(', ')}
      INTO row_data;
    `;
    var payloadNew = payloadTpl.replace(/\$ROW\$/g, 'NEW');
    var payloadOld = payloadTpl.replace(/\$ROW\$/g, 'OLD');

    var triggerName = `${channel}_${table}`;

    querySequence(conn, [
      `CREATE OR REPLACE FUNCTION ${triggerName}() RETURNS trigger AS $$
        DECLARE
          row_data RECORD;
        BEGIN
          IF (TG_OP IN ('INSERT', 'UPDATE')) THEN
            ${payloadNew}
            PERFORM pg_notify('${channel}', row_to_json(row_data)::TEXT);
          END IF;
          IF (TG_OP IN ('DELETE', 'UPDATE')) THEN
            ${payloadOld}
            PERFORM pg_notify('${channel}', row_to_json(row_data)::TEXT);
          END IF;

          RETURN NULL;
        END;
      $$ LANGUAGE plpgsql`,
      `DROP TRIGGER IF EXISTS "${triggerName}"
        ON "${table}"`,
      `CREATE TRIGGER "${triggerName}"
        AFTER INSERT OR UPDATE OR DELETE ON "${table}"
        FOR EACH ROW EXECUTE PROCEDURE ${triggerName}()`
    ], (error, results) => {
      if(error) return this.emit('error', error);
    });

  }
  forwardNotification(payload) {
    this.emit('change', payload);
  }
}

module.exports = RowTrigger;

