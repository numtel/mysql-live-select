"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var EventEmitter = require("events").EventEmitter;
var _ = require("lodash");

var murmurHash = require("../dist/murmurhash3_gc");
var querySequence = require("./querySequence");

var LiveSelect = (function (EventEmitter) {
  function LiveSelect(parent, query) {
    var _this = this;
    _classCallCheck(this, LiveSelect);

    var conn = parent.conn;
    var channel = parent.channel;


    this.query = query;
    this.conn = conn;
    this.data = {};
    this.ready = false;

    this.viewName = "" + channel + "_" + murmurHash(query);

    this.throttledRefresh = _.debounce(this.refresh, 1000, { leading: true });

    // Create view for this query
    this.createView(this.viewName, query, function (error, result) {
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

      _this.listen();

      // Grab initial results
      _this.refresh(true);
    });
  }

  _inherits(LiveSelect, EventEmitter);

  _prototypeProperties(LiveSelect, null, {
    createView: {
      value: function createView(name, query, callback) {
        var _this = this;
        var tmpName = "" + this.viewName + "_tmp";

        var primary = "\n      CASE WHEN\n        cc.column_name = vc.column_name\n      THEN 1\n      ELSE 0\n      END\n    ";

        var columnUsageQuery = "\n      SELECT\n        vc.table_name,\n        vc.column_name\n      FROM\n        information_schema.view_column_usage vc\n      WHERE\n        view_name = $1\n    ";

        var tableUsageQuery = "\n      SELECT\n        vt.table_name,\n        cc.column_name\n      FROM\n        information_schema.view_table_usage vt JOIN\n        information_schema.table_constraints tc ON\n          tc.table_catalog = vt.table_catalog AND\n          tc.table_schema = vt.table_schema AND\n          tc.table_name = vt.table_name AND\n          tc.constraint_type = 'PRIMARY KEY' JOIN\n        information_schema.constraint_column_usage cc ON\n          cc.table_catalog = tc.table_catalog AND\n          cc.table_schema = tc.table_schema AND\n          cc.table_name = tc.table_name AND\n          cc.constraint_name = tc.constraint_name\n      WHERE\n        view_name = $1\n    ";

        var sql = ["CREATE OR REPLACE TEMP VIEW " + tmpName + " AS " + query, [tableUsageQuery, [tmpName]], [columnUsageQuery, [tmpName]]];

        // Create a temporary view to figure out what columns will be used
        querySequence(this.conn, sql, function (error, result) {
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

          var keySql = _.map(keys, function (value, key) {
            return "CONCAT('" + key + "', ':', \"" + key + "\".\"" + value + "\")";
          });

          var columnSql = _.map(columns, function (col, index) {
            return "\"" + col.table + "\".\"" + col.name + "\" AS " + col.alias;
          });

          var viewQuery = query.replace(pattern, "\n        SELECT\n          CONCAT(" + keySql.join(", '|', ") + ") AS _id,\n          " + columnSql + ",\n          $1\n        FROM\n      ");

          var sql = ["DROP VIEW " + tmpName, "CREATE OR REPLACE TEMP VIEW " + _this.viewName + " AS " + viewQuery];

          querySequence(_this.conn, sql, function (error, result) {
            if (error) return callback.call(_this, error);
            return callback.call(_this, null, { keys: keys, columns: columns });
          });
        });
      },
      writable: true,
      configurable: true
    },
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
              _this.throttledRefresh(tmpRow);
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
      value: function refresh(condition) {
        var _this = this;
        // If refreshing the entire result set,
        // we don't need to run a separate ID query
        if (condition === true) {
          this.conn.query("SELECT * FROM " + this.viewName, function (error, result) {
            if (error) return _this.emit("error", error);

            var allIds = {};

            result.rows.forEach(function (row, index) {
              var id = row._id;

              allIds[id] = index;
            });

            _this.update(result.rows, allIds);
          });
        } else {
          // Run a separate query to get all IDs and their indexes
          this.conn.query("SELECT _id FROM " + this.viewName, function (error, result) {
            if (error) return _this.emit("error", error);

            var allIds = {};

            result.rows.forEach(function (row, index) {
              var id = row._id;

              allIds[id] = index;
            });

            var valueCount = 0;
            var values = _.values(condition);

            // Build WHERE clause if not refreshing entire result set
            var where = _.keys(condition).map(function (key, index) {
              return "" + key + " = $" + (index + 1);
            }).join(" AND ");

            var sql = "SELECT * FROM " + _this.viewName + " WHERE " + where;

            _this.conn.query(sql, values, function (error, result) {
              if (error) return _this.emit("error", error);

              _this.update(result.rows, allIds);
            });
          });
        }
      },
      writable: true,
      configurable: true
    },
    update: {
      value: function update(rows, allIds) {
        var _this = this;
        var diff = [];

        // Handle added/changed rows
        rows.forEach(function (row) {
          var id = row._id;

          if (_this.data[id]) {
            // If this row existed in the result set,
            // check to see if anything has changed
            var hasDiff = false;

            for (var col in _this.data[id]) {
              if (_this.data[id][col] !== row[col]) {
                hasDiff = true;
                break;
              }
            }

            hasDiff && diff.push(["changed", _this.data[id], row]);
          } else {
            // Otherwise, it was added
            diff.push(["added", row]);
          }

          _this.data[id] = row;
        });

        // Check to see if there are any
        // IDs that have been removed
        _.forOwn(this.data, function (row, id) {
          if (_.isUndefined(allIds[id])) {
            diff.push(["removed", row]);
            delete _this.data[id];
          }
        });

        if (diff.length !== 0) {
          // Output all difference events in a single event
          this.emit("update", diff, this.data);
        }
      },
      writable: true,
      configurable: true
    }
  });

  return LiveSelect;
})(EventEmitter);

module.exports = LiveSelect;