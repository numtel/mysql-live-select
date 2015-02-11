var EventEmitter = require('events').EventEmitter;
var _            = require('lodash');

var murmurHash = require('../dist/murmurhash3_gc');
var querySequence = require('./querySequence');

class LiveSelect extends EventEmitter {
  constructor(parent, query) {
    var { conn, channel } = parent;

    this.query = query;
    this.conn = conn;
    this.data = {};
    this.ready = false;

    this.viewName = `${channel}_${murmurHash(query)}`;

    this.throttledRefresh = _.debounce(this.refresh, 1000, { leading: true });

    // Create view for this query
    createView.call(this, this.viewName, query, (error, result) => {
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

      this.triggers = _.map(triggers,
        (columns, table) => parent.createTrigger(table, columns));

      this.aliases = aliases;

      this.listen();

      // Grab initial results
      this.refresh(true);
    });
  }

  listen() {
    this.triggers.forEach((trigger) => {
      trigger.on('change', (payload) => {
        // Update events contain both old and new values in payload
        // using 'new_' and 'old_' prefixes on the column names
        var argVals = {};

        if(payload._op === 'UPDATE') {
          trigger.payloadColumns.forEach((col) => {
            if(payload[`new_${col}`] !== payload[`old_${col}`]) {
              argVals[col] = payload[`new_${col}`];
            }
          });
        }
        else {
          trigger.payloadColumns.forEach((col) => {
            argVals[col] = payload[col];
          });
        }

        // Generate a map denoting which rows to replace
        var tmpRow = {};

        _.forOwn(argVals, (value, column) => {
          var alias = this.aliases[trigger.table][column];
          tmpRow[alias] = value;
        });

        if(!_.isEmpty(tmpRow)) {
          this.throttledRefresh(tmpRow);
        }
      });

      trigger.on('ready', (results) => {
        // Check if all handlers are ready
        if(this.triggers.filter(trigger => !trigger.ready).length === 0){
          this.ready = true;
          this.emit('ready', results);
        }
      });
    });
  }

  refresh(condition) {
    // If refreshing the entire result set,
    // we don't need to run a separate ID query
    if(condition === true) {
      this.conn.query(`SELECT * FROM ${this.viewName}`, (error, result) => {
        if(error) return this.emit('error', error);

        var allIds = {};

        result.rows.forEach((row, index) => {
          var id = row._id;

          allIds[id] = index;
        });

        this.update(result.rows, allIds);
      });
    }
    else {
      // Run a separate query to get all IDs and their indexes
      this.conn.query(`SELECT _id FROM ${this.viewName}`, (error, result) => {
        if(error) return this.emit('error', error);

        var allIds = {};

        result.rows.forEach((row, index) => {
          var id = row._id;

          allIds[id] = index;
        });

        var valueCount = 0;
        var values     = _.values(condition);

        // Build WHERE clause if not refreshing entire result set
        var where = _.keys(condition)
            .map((key, index) => `${key} = $${index + 1}`)
            .join(' AND ');

        var sql = `SELECT * FROM ${this.viewName} WHERE ${where}`;

        this.conn.query(sql, values, (error, result) =>  {
          if(error) return this.emit('error', error);

          this.update(result.rows, allIds);
        });
      });
    }
  }

  update(rows, allIds) {
    var diff = [];

    // Handle added/changed rows
    rows.forEach((row) => {
      var id = row._id;

      if(this.data[id]) {
        // If this row existed in the result set,
        // check to see if anything has changed
        var hasDiff = false;

        for(var col in this.data[id]) {
          if(this.data[id][col] !== row[col]) {
            hasDiff = true;
            break;
          }
        }

        hasDiff && diff.push(['changed', this.data[id], row]);
      }
      else {
        // Otherwise, it was added
        diff.push(['added', row]);
      }

      this.data[id] = row;
    });

    // Check to see if there are any
    // IDs that have been removed
    _.forOwn(this.data, (row, id) => {
      if(_.isUndefined(allIds[id])) {
        diff.push(['removed', row]);
        delete this.data[id];
      }
    });

    if(diff.length !== 0){
      // Output all difference events in a single event
      this.emit('update', diff, this.data);
    }
  }
}

/**
 * Creates a view for a particular query that includes additional columns
 * @param  String   name     The name of the view
 * @param  String   query    The query
 * @param  Function callback A function that is called with information about the view
 */
function createView(name, query, callback) {
  var tmpName  = `${this.viewName}_tmp`;

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

  var sql = [
    `CREATE OR REPLACE TEMP VIEW ${tmpName} AS ${query}`,
    [tableUsageQuery, [tmpName]],
    [columnUsageQuery, [tmpName]]
  ];

  // Create a temporary view to figure out what columns will be used
  querySequence(this.conn, sql, (error, result) => {
    if(error) return callback.call(this, error);

    var tableUsage  = result[1].rows;
    var columnUsage = result[2].rows;

    var keys    = {};
    var columns = [];

    tableUsage.forEach((row, index) => {
      keys[row.table_name] = row.column_name;
    });

    // This might not be completely reliable
    var pattern = /SELECT([\s\S]+)FROM/;

    columnUsage.forEach((row, index) => {
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
      return callback.call(this, null, { keys, columns });
    });
  });
}

module.exports = LiveSelect;
