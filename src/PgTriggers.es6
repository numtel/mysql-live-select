var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');

var querySequence = require('./querySequence');
var RowTrigger = require('./RowTrigger');
var LiveSelect = require('./LiveSelect');

class PgTriggers extends EventEmitter {
  constructor(client, channel) {
    this.client = client;
    this.payloadColumnBuffer = {};
    this.channel = channel;

    this.setMaxListeners(0); // Allow unlimited listeners

    client.query(`LISTEN "${channel}"`, function(error, result) {
      if(error) throw error;
    });

    client.on('notification', (info) => {
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
  select(query, params) {
    return new LiveSelect(this, query, params);
  }
  cleanup(callback) {
    var { payloadColumnBuffer, client, channel } = this;

    var queries = [];
    _.forOwn(payloadColumnBuffer, (payloadColumns, table) => {
      var triggerName = `${channel}_${table}`;
      queries.push(`DROP TRIGGER IF EXISTS ${triggerName} ON ${table}`);
      queries.push(`DROP FUNCTION IF EXISTS ${triggerName}()`);
    });

    querySequence(client, queries, callback);
  }
}

module.exports = PgTriggers;

