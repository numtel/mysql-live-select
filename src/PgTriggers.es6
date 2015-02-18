var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');

var querySequence = require('./querySequence');
var RowTrigger = require('./RowTrigger');
var LiveSelect = require('./LiveSelect');

class PgTriggers extends EventEmitter {
  constructor(client, channel) {
    this.client        = client;
    this.channel       = channel;
    this.selects       = [];
    this.triggerTables = [];

    this.setMaxListeners(0); // Allow unlimited listeners

    client.query(`LISTEN "${channel}"`, function(error, result) {
      if(error) throw error;
    });

    client.on('notification', (info) => {
      if(info.channel === channel) {
        this.emit(`change:${info.payload}`);
      }
    });
  }

  createTrigger(table) {
    return new RowTrigger(this, table);
  }

  select(query, params) {
    var select = new LiveSelect(this, query, params);
    this.selects.push(select);
    return select;
  }

  cleanup(callback) {
    var { triggerTables, client, channel } = this;

    var queries = [];
    triggerTables.forEach(table => {
      var triggerName = `${channel}_${table}`;
      queries.push(`DROP TRIGGER IF EXISTS ${triggerName} ON ${table}`);
      queries.push(`DROP FUNCTION IF EXISTS ${triggerName}()`);
    });

    querySequence(client, queries, callback);
  }
}

module.exports = PgTriggers;

