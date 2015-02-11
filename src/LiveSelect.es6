var EventEmitter = require('events').EventEmitter;
var _            = require('lodash');

var murmurHash = require('../dist/murmurhash3_gc');
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
    this.createView(this.viewName, query, (error, result) => {
      if(error) return this.emit('error', error);

      var triggers     = {};
      var aliases      = {};
      var primary_keys = result.keys;

      result.columns.forEach((col) => {
        if(!triggers[col.table]) {
          triggers[col.table] = [];
        }

        if(!aliases[col.table]) {
          aliases[col.table] = {};
        }

        triggers[col.table].push(col.name);
        aliases[col.table][col.name] = col.alias;
      });

      this.triggers = _.map(triggers, (columns, table) => {
        return {
          handler   : parent.createTrigger(table, columns),
          columns   : columns,
          validator : (row) => {
            var tmpRow = {};

            _.forOwn(row, (value, column) => {
              var alias = aliases[table][column];
              tmpRow[alias] = value;
            });

            return tmpRow;
          }
        };
      });

      this.listen();

      // Grab initial results
      this.refresh(true);
    });
  }

  createView(name, query, callback) {
    var tmpName  = `${this.viewName}_tmp`;

    var primary = `
      CASE WHEN
        cc.column_name = vc.column_name
      THEN 1
      ELSE 0
      END
    `;

    var columnUsageQuery = `
      SELECT
        vc.table_name,
        vc.column_name
      FROM
        information_schema.view_column_usage vc
      WHERE
        view_name = $1
    `;

    var tableUsageQuery = `
      SELECT
        vt.table_name,
        cc.column_name
      FROM
        information_schema.view_table_usage vt JOIN
        information_schema.table_constraints tc ON
          tc.table_catalog = vt.table_catalog AND
          tc.table_schema = vt.table_schema AND
          tc.table_name = vt.table_name AND
          tc.constraint_type = 'PRIMARY KEY' JOIN
        information_schema.constraint_column_usage cc ON
          cc.table_catalog = tc.table_catalog AND
          cc.table_schema = tc.table_schema AND
          cc.table_name = tc.table_name AND
          cc.constraint_name = tc.constraint_name
      WHERE
        view_name = $1
    `;

    // Create a temporary view to figure out what columns will be used
    this.conn.query(`CREATE OR REPLACE TEMP VIEW ${tmpName} AS ${query}`,
      (error, results) => {
        this.conn.query(tableUsageQuery, [tmpName], (error, result) => {
          if(error) return callback.call(this, error);

          var keys    = {};
          var columns = [];

          result.rows.forEach((row, index) => {
            keys[row.table_name] = row.column_name;
          });

          this.conn.query(columnUsageQuery, [tmpName], (error, result) => {
            if(error) return callback.call(this, error);

            // This might not be completely reliable
            var pattern = /SELECT([\s\S]+)FROM/;

            result.rows.forEach((row, index) => {
              columns.push({
                table : row.table_name,
                name  : row.column_name,
                alias : `_${row.table_name}_${row.column_name}`
              });
            });

            var keySql = _.map(keys,
              (value, key) => `CONCAT('${key}', ':', "${key}"."${value}")`);

            var columnSql = _.map(columns,
              (col, index) => `"${col.table}"."${col.name}" AS ${col.alias}`);

            var viewQuery = query.replace(pattern, `
              SELECT
                CONCAT(${keySql.join(", '|', ")}) AS _id,
                ${columnSql},
                $1
              FROM
            `);

            var sql = [
              `DROP VIEW ${tmpName}`,
              `CREATE OR REPLACE TEMP VIEW ${this.viewName} AS ${viewQuery}`
            ];

            querySequence(this.conn, sql, (error, result) =>  {
              if(error) return callback.call(this, error);
              return callback.call(this, null, {keys, columns});
            });
          });
        });
    });
  }

  listen() {
    this.triggers.forEach((trigger) => {
      trigger.handler.on('change', (payload) => {
        // Update events contain both old and new values in payload
        // using 'new_' and 'old_' prefixes on the column names
        var prefix  = payload._op === 'UPDATE' ? 'new_' : '';
        var argVals = {};

        if(payload._op === 'UPDATE') {
          trigger.columns.forEach((col) => {
            if(payload[`new_${col}`] !== payload[`old_${col}`]) {
              argVals[col] = payload[`new_${col}`];
            }
          });
        }
        else {
          trigger.columns.forEach((col) => {
            argVals[col] = payload[col];
          });
        }

        // Validator lambdas return {key:value}
        // map denoting which rows to replace
        var refresh = trigger.validator.call(this, argVals);

        if(!_.isEmpty(refresh)) {
          this.throttledRefresh(refresh);
        }
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

