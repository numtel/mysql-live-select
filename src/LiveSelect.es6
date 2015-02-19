var EventEmitter = require('events').EventEmitter;
var _            = require('lodash');
var deep         = require('deep-diff');

var murmurHash    = require('../dist/murmurhash3_gc');
var querySequence = require('./querySequence');
var RowCache      = require('./RowCache');
var cachedQueries = {};
var cache         = new RowCache();

const THROTTLE_INTERVAL = 1000;

class LiveSelect extends EventEmitter {
  constructor(parent, query, params) {
    var { client, channel } = parent;

    this.params  = params;
    this.client  = client;
    this.data    = [];
    this.hashes  = [];
    this.ready   = false;
    this.stopped = false;

    // throttledRefresh method buffers
    this.throttledRefresh = _.debounce(this.refresh, THROTTLE_INTERVAL);

    // Create view for this query
    addHelpers.call(this, query, (error, result) => {
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
      this.query   = result.query;

      this.listen();

      // Grab initial results
      this.refresh();
    });
  }

  listen() {
    this.triggers.forEach(trigger => {
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
          this.throttledRefresh();
        }
      });

      trigger.on('ready', (results) => {
        // Check if all handlers are ready
        if(this.triggers.filter(trigger => !trigger.ready).length === 0) {
          this.ready = true;
          this.emit('ready', results);
        }
      });
    });
  }

  refresh() {
    // Run a query to get an updated hash map
    var sql = `
      WITH tmp AS (${this.query})
      SELECT
        tmp2._hash
      FROM
        (
          SELECT
            MD5(CAST(tmp.* AS TEXT)) AS _hash
          FROM
            tmp
        ) tmp2
      ORDER BY
        tmp2._hash DESC
    `;

    this.client.query(sql, this.params, (error, result) =>  {
      if(error) return this.emit('error', error);

      var hashes = _.pluck(result.rows, '_hash');
      var diff   = deep.diff(this.hashes, hashes);
      var fetch  = {};

      // If nothing has changed, stop here
      if(!diff) {
        return;
      }

      // Build a list of changes and hashes to fetch
      var changes = diff.map(change => {
        var tmpChange = {};

        if(change.kind === 'E') {
          _.extend(tmpChange, {
            type   : 'changed',
            index  : change.path.pop(),
            oldKey : change.lhs,
            newKey : change.rhs
          });

          if(!cache.get(tmpChange.oldKey)) {
            fetch[tmpChange.oldKey] = true;
          }

          if(!cache.get(tmpChange.newKey)) {
            fetch[tmpChange.newKey] = true;
          }
        }
        else if(change.kind === 'A') {
          _.extend(tmpChange, {
            index : change.index
          })

          if(change.item.kind === 'N') {
            tmpChange.type = 'added';
            tmpChange.key  = change.item.rhs;
          }
          else {
            tmpChange.type = 'removed';
            tmpChange.key  = change.item.lhs;
          }

          if(!cache.get(tmpChange.key)) {
            fetch[tmpChange.key] = true;
          }
        }
        else {
          throw new Error(`Unrecognized change: ${JSON.stringify(change)}`);
        }

        return tmpChange;
      });

      if(_.isEmpty(fetch)) {
        this.update(changes);
      }
      else {
        var sql = `
          WITH tmp AS (${this.query})
          SELECT
            tmp2.*
          FROM
            (
              SELECT
                MD5(CAST(tmp.* AS TEXT)) AS _hash,
                tmp.*
              FROM
                tmp
            ) tmp2
          WHERE
            tmp2._hash IN ('${_.keys(fetch).join("', '")}')
          ORDER BY
            tmp2._hash DESC
        `;

        // Fetch hashes that aren't in the cache
        this.client.query(sql, this.params, (error, result) => {
          if(error) return this.emit('error', error);
          result.rows.forEach(row => cache.add(row._hash, row));

          this.update(changes);
        });

        // Store the current hash map
        this.hashes = hashes;
      }
    });
  }

  update(changes) {
    var remove = [];

    // Emit an update event with the changes
    this.emit('update', changes.map(change => {
      var args = [change.type];

      if(change.type === 'added') {
        var row = cache.get(change.key);

        args.push(change.index, row);
      }
      else if(change.type === 'changed') {
        var oldRow = cache.get(change.oldKey);
        var newRow = cache.get(change.newKey);

        args.push(change.index, oldRow, newRow);
        remove.push(change.oldKey);
      }
      else if(change.type === 'removed') {
        var row = cache.get(change.key);

        args.push(change.index, row);
        remove.push(change.key);
      }

      return args;
    }));

    remove.forEach(key => cache.remove(key));
  }

  stop(callback) {
    if(this.stopped) {
      return callback();
    }

    this.triggers.forEach(trigger => trigger.stop((error, result) => {
      var stopped = !this.triggers.filter(trigger => !trigger.stopped).length;

      if(stopped) {
        this.stopped = true;
        callback();
      }
    }));
  }
}

/**
 * Adds helper columns to a query
 * @context LiveSelect instance
 * @param   String   query    The query
 * @param   Function callback A function that is called with information about the view
 */
function addHelpers(query, callback) {
  var hash = murmurHash(query);

  // If this query was cached before, reuse it
  if(cachedQueries[hash]) {
    return callback(null, cachedQueries[hash]);
  }

  var tmpName = `tmp_view_${hash}`;

  var columnUsageSQL = `
    SELECT DISTINCT
      vc.table_name,
      vc.column_name
    FROM
      information_schema.view_column_usage vc
    WHERE
      view_name = $1
  `;

  var tableUsageSQL = `
    SELECT DISTINCT
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

  // Replace all parameter values with NULL
  var tmpQuery      = query.replace(/\$\d/g, 'NULL');
  var createViewSQL = `CREATE OR REPLACE TEMP VIEW ${tmpName} AS (${tmpQuery})`;

  var columnUsageQuery = {
    name   : 'column_usage_query',
    text   : columnUsageSQL,
    values : [tmpName]
  };

  var tableUsageQuery = {
    name   : 'table_usage_query',
    text   : tableUsageSQL,
    values : [tmpName]
  };

  var sql = [
    `CREATE OR REPLACE TEMP VIEW ${tmpName} AS (${tmpQuery})`,
    tableUsageQuery,
    columnUsageQuery
  ];

  // Create a temporary view to figure out what columns will be used
  querySequence(this.client, sql, (error, result) => {
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

    cachedQueries[hash] = { keys, columns, query };

    return callback(null, cachedQueries[hash]);
  });
}

module.exports = LiveSelect;
