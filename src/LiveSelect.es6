var EventEmitter = require('events').EventEmitter;
var _            = require('lodash');

var getFunctionArgumentNames = require('./getFunctionArgumentNames');

class LiveSelect extends EventEmitter {
  constructor(parent, query, triggers) {
    var { conn } = parent;

    this.query = query;
    this.triggers = triggers;
    this.conn = conn;
    this.data = [];
    this.ready = false;

    this.throttledRefresh = _.debounce(this.refresh, 1000, { leading: true });

    this.triggerHandlers = _.map(triggers, (handler, table) => 
      parent.createTrigger(table, getFunctionArgumentNames(handler)));

    this.triggerHandlers.forEach((handler) => {
      handler.on('change', (payload) => {
        var validator = triggers[handler.table];
        var args = getFunctionArgumentNames(validator);
        if(payload._op === 'UPDATE') {
          // Update events contain both old and new values in payload
          // using 'new_' and 'old_' prefixes on the column names
          var argNewVals = args.map(arg => payload[`new_${arg}`]);
          var argOldVals = args.map(arg => payload[`old_${arg}`]);

          if(validator.apply(this, argNewVals) ||
             validator.apply(this, argOldVals)){
            this.throttledRefresh();
          }
        }else{
          // Insert and Delete events do not have prefixed column names
          var argVals = args.map(arg => payload[arg]);
          if(validator.apply(this, argVals)) this.throttledRefresh();
        }
      });

      handler.on('ready', (results) => {
        // Check if all handlers are ready
        if(this.triggerHandlers.filter(handler => !handler.ready).length === 0){
          this.ready = true;
          this.emit('ready', results);
        }
      });
    });

    // Grab initial results
    this.refresh();
  }
  refresh() {
    this.conn.query(this.query, (error, results) => {
      if(error) return this.emit('error', error);
      var rows = results.rows;

      if(this.listeners('diff').length !== 0) {
        var diff = [];
        rows.forEach((row, index) => {
          if(this.data.length - 1 < index){
            diff.push(['added', row, index]);
          }else if(JSON.stringify(this.data[index]) !== JSON.stringify(row)){
            diff.push(['changed', this.data[index], row, index]);
          }
        });

        if(this.data.length > rows.length){
          for(var i = this.data.length - 1; i >= rows.length; i--){
            diff.push(['removed', this.data[i], i]);
          }
        }
        if(diff.length !== 0){
          // Output all difference events in a single event
          this.emit('diff', diff);
        }
      }

      this.data = rows;
      this.emit('update', rows);
    });
  }
}

module.exports = LiveSelect;

