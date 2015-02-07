var EventEmitter = require('events').EventEmitter;
var _            = require('lodash');

var getFunctionArgumentNames = require('./getFunctionArgumentNames');

class LiveSelect extends EventEmitter {
  constructor(parent, query, triggers) {
    var { conn } = parent;

    this.query = query;
    this.triggers = triggers;
    this.conn = conn;

    this.throttledRefresh = _.debounce(this.refresh, 1000, {
      leading: true
    });

    this.triggerHandlers = _.map(triggers, (handler, table) => 
      parent.createTrigger(table, getFunctionArgumentNames(handler)));

    this.triggerHandlers.forEach((handler) => {
      // TODO: Fix so that if both UPDATE NEW and UPDATE OLD triggers match,
      //        the results are only updated one time
      handler.on('change', (payload) => {
        var validator = triggers[handler.table];
        var args = getFunctionArgumentNames(validator);
        var argVals = args.map(arg => payload[arg]);
        if(validator.apply(this, argVals)) this.throttledRefresh();
      });
    });

    // Grab initial results
    this.refresh();
  }
  refresh() {
    this.conn.query(this.query, (error, results) => {
      if(error) return this.emit('error', error);
      this.emit('update', results.rows);
    });
  }
}

module.exports = LiveSelect;

