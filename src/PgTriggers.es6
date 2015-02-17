var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');

var querySequence = require('./querySequence');
var RowTrigger = require('./RowTrigger');
var LiveSelect = require('./LiveSelect');

class PgTriggers extends EventEmitter {
  constructor(client, channel) {
    this.client  = client;
    this.channel = channel;
    this.stopped = false;
    this.selects = [];

    this.payloadColumnBuffer = {};
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
    var select = new LiveSelect(this, query, params);
    this.selects.push(select);
    return select;
  }

  stop(callback) {
    if(this.stopped) {
      return callback();
    }

    this.selects.forEach(select => select.stop(() => {
      var stopped = !this.selects.filter(select => !select.stopped).length;

      if(stopped) {
        this.stopped = true;
        callback();
      }
    }));
  }
}

module.exports = PgTriggers;

