var EventEmitter = require('events').EventEmitter;
var _            = require('lodash');

var murmurHash = require('../dist/murmurhash3_gc');

var getFunctionArgumentNames = require('./getFunctionArgumentNames');
var querySequence = require('./querySequence');

class LiveSelect extends EventEmitter {
  constructor(parent, query, triggers) {
    var { conn, channel } = parent;

    this.query = query;
    this.triggers = triggers;
    this.conn = conn;
    this.data = [];
    this.ready = false;

    this.viewName = `${channel}_${murmurHash(query)}`;

    this.throttledRefresh = _.debounce(this.refresh, 1000, { leading: true });

    this.triggerHandlers = _.map(triggers, (handler, table) => 
      parent.createTrigger(table, getFunctionArgumentNames(handler)));

    this.triggerHandlers.forEach((handler) => {
      handler.on('change', (payload) => {
        var validator = triggers[handler.table];
        var args = getFunctionArgumentNames(validator);
        // Validator lambdas may return false to skip refresh,
        //  true to refresh entire result set, or
        //  {key:value} map denoting which rows to replace
        var refresh;
        if(payload._op === 'UPDATE') {
          // Update events contain both old and new values in payload
          // using 'new_' and 'old_' prefixes on the column names
          var argNewVals = args.map(arg => payload[`new_${arg}`]);
          var argOldVals = args.map(arg => payload[`old_${arg}`]);

          refresh = validator.apply(this, argNewVals);
          if(refresh === false) {
            // Try old values as well
            refresh = validator.apply(this, argOldVals);
          }
        }else{
          // Insert and Delete events do not have prefixed column names
          var argVals = args.map(arg => payload[arg]);
          refresh = validator.apply(this, argVals);
        }

        refresh && this.throttledRefresh(refresh);
      });

      handler.on('ready', (results) => {
        // Check if all handlers are ready
        if(this.triggerHandlers.filter(handler => !handler.ready).length === 0){
          this.ready = true;
          this.emit('ready', results);
        }
      });
    });

    // Create view for this query
    this.conn.query(`CREATE OR REPLACE TEMP VIEW ${this.viewName} AS ${query}`,
      (error, results) => {
        if(error) return this.emit('error', error);

        // Grab initial results
        this.refresh(true);
    });

  }
  refresh(condition) {
    // Build WHERE clause if not refreshing entire result set
    var values, where;
    if(condition !== true) {
      var valueCount = 0;
      values = _.values(condition);
      where = 'WHERE ' +
        _.keys(condition)
          .map((key, index) => `${key} = $${index + 1}`)
          .join(' AND ');
    }else{
      values = [];
      where  = '';
    }

    this.conn.query(`SELECT * FROM ${this.viewName} ${where}`, values,
      (error, results) => {
        if(error) return this.emit('error', error);
        var rows;
        if(condition !== true) {
          // Do nothing if no change
          if(results.rows.length === 0) return;
          // Partial refresh, copy rows from current data
          rows = this.data.slice();
          _.forOwn(condition, (value, key) => {
            // Only keep rows that do not match the condition value on key
            rows = rows.filter(row => row[key] !== value);
          });
          // Append new data
          rows = rows.concat(results.rows);
        }else{
          rows = results.rows;
        }

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
      }
    );
  }
}

module.exports = LiveSelect;

