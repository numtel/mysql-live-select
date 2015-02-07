var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');

var querySequence = require('./querySequence');
var RowTrigger = require('./RowTrigger');
var LiveSelect = require('./LiveSelect');

class PgTriggers extends EventEmitter {
  constructor(conn, channel) {
    this.conn = conn;
    this.payloadColumnBuffer = {};
    this.channel = channel;

    conn.query(`LISTEN "${channel}"`, function(error, result) {
      if(error) throw error;
    });

    conn.on('notification', (info) => {
      if(info.channel === channel) {
        try {
          var payload = JSON.parse(info.payload);
        } catch(err) {
          return this.emit('error', new Error('Malformed payload!'));
        }

        this.emit(`change:${payload._table}`, payload);
      }
    });
  }
  createTrigger(table, payloadColumns) {
    return new RowTrigger(this, table, payloadColumns);
  }
  select(query, triggers) {
    return new LiveSelect(this, query, triggers);
  }
  cleanup(callback) {
    var { payloadColumnBuffer, conn, channel } = this;

    var queries = [];
    _.forOwn(payloadColumnBuffer, (payloadColumns, table) => {
      var triggerName = `${channel}_${table}`;
      queries.push(`DROP TRIGGER IF EXISTS ${triggerName} ON ${table}`);
      queries.push(`DROP FUNCTION IF EXISTS ${triggerName}()`);
    });

    querySequence(conn, queries, callback);
  }
}

module.exports = PgTriggers;

