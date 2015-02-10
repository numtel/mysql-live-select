var EventEmitter = require('events').EventEmitter;
var _            = require('lodash');

var murmurHash = require('../dist/murmurhash3_gc');

var getFunctionArgumentNames = require('./getFunctionArgumentNames');
var querySequence = require('./querySequence');

class LiveSelect extends EventEmitter {
  constructor(parent, query) {
    var { conn, channel } = parent;

    this.query = query;
    this.conn = conn;
    this.data = [];
    this.ready = false;

    this.viewName = `${channel}_${murmurHash(query)}`;

    this.throttledRefresh = _.debounce(this.refresh, 1000, { leading: true });

    // Create view for this query
    this.conn.query(`CREATE OR REPLACE TEMP VIEW ${this.viewName} AS ${query}`,
      (error, results) => {
        if(error) return this.emit('error', error);

        // Generate triggers based on what we know
        // about the view from the information schema.
        var primary = `
          CASE WHEN cc.column_name = vc.column_name THEN 1 ELSE 0 END
        `;

        var sql = `
          SELECT
            vc.*,
            (${primary}) AS primary
          FROM
            information_schema.view_column_usage vc JOIN
            information_schema.table_constraints tc ON
              tc.table_catalog = vc.table_catalog AND
              tc.table_schema = vc.table_schema AND
              tc.table_name = vc.table_name AND
              tc.constraint_type = 'PRIMARY KEY' JOIN
            information_schema.constraint_column_usage cc ON
              cc.table_catalog = tc.table_catalog AND
              cc.table_schema = tc.table_schema AND
              cc.table_name = tc.table_name AND
              cc.constraint_name = tc.constraint_name
          WHERE
            view_name = '${this.viewName}'
        `;

        conn.query(sql, (error, result) => {
          if(error) return this.emit('error', error);

          var triggers     = {};
          var primary_keys = {};

          result.rows.forEach((row) => {
            var table_name  = row.table_name;
            var column_name = row.column_name;

            if(!triggers[table_name]) {
              triggers[table_name] = [];
            }

            if(row.primary) {
              primary_keys[table_name] = column_name;
            }

            triggers[table_name].push(column_name);
          });

          this.triggers = _.map(triggers, (columns, table) => {
            return {
              handler   : parent.createTrigger(table, columns),
              columns   : columns,
              validator : (...values) => _.object(columns, values)
            };
          });

          this.listen();
        });

        // Grab initial results
        this.refresh(true);
    });
  }

  listen() {
    this.triggers.forEach((trigger) => {
      trigger.handler.on('change', (payload) => {
        // Validator lambdas may return false to skip refresh,
        //  true to refresh entire result set, or
        //  {key:value} map denoting which rows to replace
        var refresh;
        if(payload._op === 'UPDATE') {
          // Update events contain both old and new values in payload
          // using 'new_' and 'old_' prefixes on the column names
          var argNewVals = trigger.columns.map(arg => payload[`new_${arg}`]);
          var argOldVals = trigger.columns.map(arg => payload[`old_${arg}`]);

          refresh = trigger.validator.apply(this, argNewVals);
          if(refresh === false) {
            // Try old values as well
            refresh = trigger.validator.apply(this, argOldVals);
          }
        }else{
          // Insert and Delete events do not have prefixed column names
          var argVals = trigger.columns.map(arg => payload[arg]);
          refresh = trigger.validator.apply(this, argVals);
        }

        refresh && this.throttledRefresh(refresh);
      });

      trigger.handler.on('ready', (results) => {
        // Check if all handlers are ready
        if(this.triggers.filter(trigger => !trigger.handler.ready).length === 0){
          this.ready = true;
          this.emit('ready', results);
        }
      });
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

