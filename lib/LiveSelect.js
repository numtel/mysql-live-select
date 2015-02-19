"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var EventEmitter = require("events").EventEmitter;
var _ = require("lodash");
var deep = require("deep-diff");

var murmurHash = require("../dist/murmurhash3_gc");
var querySequence = require("./querySequence");
var RowCache = require("./RowCache");
var cachedQueries = {};
var cache = new RowCache();

var THROTTLE_INTERVAL = 200;
var MAX_CONDITIONS = 3500;


var LiveSelect = (function (EventEmitter) {
  function LiveSelect(parent, query, params) {
    var _this = this;
    _classCallCheck(this, LiveSelect);

    var client = parent.client;
    var channel = parent.channel;


    this.params = params;
    this.client = client;
    this.data = [];
    this.hashes = [];
    this.ready = false;
    this.stopped = false;

    // throttledRefresh method buffers
    this.refreshQueue = [];
    this.throttledRefresh = _.debounce(this.refresh, THROTTLE_INTERVAL);

    // Create view for this query
    addHelpers.call(this, query, function (error, result) {
      if (error) return _this.emit("error", error);

      var triggers = {};
      var aliases = {};
      var primary_keys = result.keys;

      result.columns.forEach(function (col) {
        if (!triggers[col.table]) {
          triggers[col.table] = [];
        }

        if (!aliases[col.table]) {
          aliases[col.table] = {};
        }

        triggers[col.table].push(col.name);
        aliases[col.table][col.name] = col.alias;
      });

      _this.triggers = _.map(triggers, function (columns, table) {
        return parent.createTrigger(table, columns);
      });

      _this.aliases = aliases;
      _this.query = result.query;

      _this.listen();

      // Grab initial results
      _this.refresh(true);
    });
  }

  _inherits(LiveSelect, EventEmitter);

  _prototypeProperties(LiveSelect, null, {
    listen: {
      value: function listen() {
        var _this = this;
        this.triggers.forEach(function (trigger) {
          trigger.on("change", function (payload) {
            // Update events contain both old and new values in payload
            // using 'new_' and 'old_' prefixes on the column names
            var argVals = {};

            if (payload._op === "UPDATE") {
              trigger.payloadColumns.forEach(function (col) {
                if (payload["new_" + col] !== payload["old_" + col]) {
                  argVals[col] = payload["new_" + col];
                }
              });
            } else {
              trigger.payloadColumns.forEach(function (col) {
                argVals[col] = payload[col];
              });
            }

            // Generate a map denoting which rows to replace
            var tmpRow = {};

            _.forOwn(argVals, function (value, column) {
              var alias = _this.aliases[trigger.table][column];
              tmpRow[alias] = value;
            });

            if (!_.isEmpty(tmpRow)) {
              _this.refreshQueue.push(tmpRow);

              if (MAX_CONDITIONS && _this.refreshQueue.length >= MAX_CONDITIONS) {
                _this.refresh();
              } else {
                _this.throttledRefresh();
              }
            }
          });

          trigger.on("ready", function (results) {
            // Check if all handlers are ready
            if (_this.triggers.filter(function (trigger) {
              return !trigger.ready;
            }).length === 0) {
              _this.ready = true;
              _this.emit("ready", results);
            }
          });
        });
      },
      writable: true,
      configurable: true
    },
    refresh: {
      value: function refresh(initial) {
        var _this = this;
        // Run a query to get an updated hash map
        var sql = "\n      WITH tmp AS (" + this.query + ")\n      SELECT\n        tmp2._hash\n      FROM\n        (\n          SELECT\n            MD5(CAST(tmp.* AS TEXT)) AS _hash\n          FROM\n            tmp\n        ) tmp2\n      ORDER BY\n        tmp2._hash DESC\n    ";

        this.client.query(sql, this.params, function (error, result) {
          if (error) return _this.emit("error", error);

          var hashes = _.pluck(result.rows, "_hash");
          var diff = deep.diff(_this.hashes, hashes);
          var fetch = {};

          // If nothing has changed, stop here
          if (!diff) {
            return;
          }

          // Build a list of changes and hashes to fetch
          var changes = diff.map(function (change) {
            var tmpChange = {};

            if (change.kind === "E") {
              _.extend(tmpChange, {
                type: "changed",
                index: change.path.pop(),
                oldKey: change.lhs,
                newKey: change.rhs
              });

              if (!cache.get(tmpChange.oldKey)) {
                fetch[tmpChange.oldKey] = true;
              }

              if (!cache.get(tmpChange.newKey)) {
                fetch[tmpChange.newKey] = true;
              }
            } else if (change.kind === "A") {
              _.extend(tmpChange, {
                index: change.index
              });

              if (change.item.kind === "N") {
                tmpChange.type = "added";
                tmpChange.key = change.item.rhs;
              } else {
                tmpChange.type = "removed";
                tmpChange.key = change.item.lhs;
              }

              if (!cache.get(tmpChange.key)) {
                fetch[tmpChange.key] = true;
              }
            } else {
              throw new Error("Unrecognized change: " + JSON.stringify(change));
            }

            return tmpChange;
          });

          if (_.isEmpty(fetch)) {
            _this.update(changes);
          } else {
            var sql = "\n          WITH tmp AS (" + _this.query + ")\n          SELECT\n            tmp2.*\n          FROM\n            (\n              SELECT\n                MD5(CAST(tmp.* AS TEXT)) AS _hash,\n                tmp.*\n              FROM\n                tmp\n            ) tmp2\n          WHERE\n            tmp2._hash IN ('" + _.keys(fetch).join("', '") + "')\n          ORDER BY\n            tmp2._hash DESC\n        ";

            // Fetch hashes that aren't in the cache
            _this.client.query(sql, _this.params, function (error, result) {
              if (error) return _this.emit("error", error);
              result.rows.forEach(function (row) {
                return cache.add(row._hash, row);
              });

              _this.update(changes);
            });

            // Store the current hash map
            _this.hashes = hashes;
          }
        });
      },
      writable: true,
      configurable: true
    },
    update: {
      value: function update(changes) {
        var remove = [];

        // Emit an update event with the changes
        this.emit("update", changes.map(function (change) {
          var args = [change.type];

          if (change.type === "added") {
            var row = cache.get(change.key);

            args.push(change.index, row);
          } else if (change.type === "changed") {
            var oldRow = cache.get(change.oldKey);
            var newRow = cache.get(change.newKey);

            args.push(change.index, oldRow, newRow);
            remove.push(change.oldKey);
          } else if (change.type === "removed") {
            var row = cache.get(change.key);

            args.push(change.index, row);
            remove.push(change.key);
          }

          return args;
        }));

        remove.forEach(function (key) {
          return cache.remove(key);
        });
      },
      writable: true,
      configurable: true
    },
    flush: {
      value: function flush() {
        if (this.refreshQueue.length) {
          refresh(this.refreshQueue);
          this.refreshQueue = [];
        }
      },
      writable: true,
      configurable: true
    },
    stop: {
      value: function stop(callback) {
        var _this = this;
        if (this.stopped) {
          return callback();
        }

        this.triggers.forEach(function (trigger) {
          return trigger.stop(function (error, result) {
            var stopped = !_this.triggers.filter(function (trigger) {
              return !trigger.stopped;
            }).length;

            if (stopped) {
              _this.stopped = true;
              callback();
            }
          });
        });
      },
      writable: true,
      configurable: true
    }
  });

  return LiveSelect;
})(EventEmitter);

/**
 * Adds helper columns to a query
 * @context LiveSelect instance
 * @param   String   query    The query
 * @param   Function callback A function that is called with information about the view
 */
function addHelpers(query, callback) {
  var _this = this;
  var hash = murmurHash(query);

  // If this query was cached before, reuse it
  if (cachedQueries[hash]) {
    return callback(null, cachedQueries[hash]);
  }

  var tmpName = "tmp_view_" + hash;

  var columnUsageSQL = "\n    SELECT DISTINCT\n      vc.table_name,\n      vc.column_name\n    FROM\n      information_schema.view_column_usage vc\n    WHERE\n      view_name = $1\n  ";

  var tableUsageSQL = "\n    SELECT DISTINCT\n      vt.table_name,\n      cc.column_name\n    FROM\n      information_schema.view_table_usage vt JOIN\n      information_schema.table_constraints tc ON\n        tc.table_catalog = vt.table_catalog AND\n        tc.table_schema = vt.table_schema AND\n        tc.table_name = vt.table_name AND\n        tc.constraint_type = 'PRIMARY KEY' JOIN\n      information_schema.constraint_column_usage cc ON\n        cc.table_catalog = tc.table_catalog AND\n        cc.table_schema = tc.table_schema AND\n        cc.table_name = tc.table_name AND\n        cc.constraint_name = tc.constraint_name\n    WHERE\n      view_name = $1\n  ";

  // Replace all parameter values with NULL
  var tmpQuery = query.replace(/\$\d/g, "NULL");
  var createViewSQL = "CREATE OR REPLACE TEMP VIEW " + tmpName + " AS (" + tmpQuery + ")";

  var columnUsageQuery = {
    name: "column_usage_query",
    text: columnUsageSQL,
    values: [tmpName]
  };

  var tableUsageQuery = {
    name: "table_usage_query",
    text: tableUsageSQL,
    values: [tmpName]
  };

  var sql = ["CREATE OR REPLACE TEMP VIEW " + tmpName + " AS (" + tmpQuery + ")", tableUsageQuery, columnUsageQuery];

  // Create a temporary view to figure out what columns will be used
  querySequence(this.client, sql, function (error, result) {
    if (error) return callback.call(_this, error);

    var tableUsage = result[1].rows;
    var columnUsage = result[2].rows;

    var keys = {};
    var columns = [];

    tableUsage.forEach(function (row, index) {
      keys[row.table_name] = row.column_name;
    });

    // This might not be completely reliable
    var pattern = /SELECT([\s\S]+)FROM/;

    columnUsage.forEach(function (row, index) {
      columns.push({
        table: row.table_name,
        name: row.column_name,
        alias: "_" + row.table_name + "_" + row.column_name
      });
    });

    cachedQueries[hash] = { keys: keys, columns: columns, query: query };

    return callback(null, cachedQueries[hash]);
  });
}

module.exports = LiveSelect;