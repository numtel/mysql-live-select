"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var EventEmitter = require("events").EventEmitter;
var _ = require("lodash");

var murmurHash = require("../dist/murmurhash3_gc");

var getFunctionArgumentNames = require("./getFunctionArgumentNames");
var querySequence = require("./querySequence");

var LiveSelect = (function (EventEmitter) {
  function LiveSelect(parent, query, triggers) {
    var _this = this;
    _classCallCheck(this, LiveSelect);

    var conn = parent.conn;
    var channel = parent.channel;


    this.query = query;
    this.triggers = triggers;
    this.conn = conn;
    this.data = [];
    this.ready = false;

    this.viewName = "" + channel + "_" + murmurHash(query);

    this.throttledRefresh = _.debounce(this.refresh, 1000, { leading: true });

    this.triggerHandlers = _.map(triggers, function (handler, table) {
      return parent.createTrigger(table, getFunctionArgumentNames(handler));
    });

    this.triggerHandlers.forEach(function (handler) {
      handler.on("change", function (payload) {
        var validator = triggers[handler.table];
        var args = getFunctionArgumentNames(validator);
        // Validator lambdas may return false to skip refresh,
        //  true to refresh entire result set, or
        //  {key:value} map denoting which rows to replace
        var refresh;
        if (payload._op === "UPDATE") {
          // Update events contain both old and new values in payload
          // using 'new_' and 'old_' prefixes on the column names
          var argNewVals = args.map(function (arg) {
            return payload["new_" + arg];
          });
          var argOldVals = args.map(function (arg) {
            return payload["old_" + arg];
          });

          refresh = validator.apply(_this, argNewVals);
          if (refresh === false) {
            // Try old values as well
            refresh = validator.apply(_this, argOldVals);
          }
        } else {
          // Insert and Delete events do not have prefixed column names
          var argVals = args.map(function (arg) {
            return payload[arg];
          });
          refresh = validator.apply(_this, argVals);
        }

        refresh && _this.throttledRefresh(refresh);
      });

      handler.on("ready", function (results) {
        // Check if all handlers are ready
        if (_this.triggerHandlers.filter(function (handler) {
          return !handler.ready;
        }).length === 0) {
          _this.ready = true;
          _this.emit("ready", results);
        }
      });
    });

    // Create view for this query
    this.conn.query("CREATE OR REPLACE TEMP VIEW " + this.viewName + " AS " + query, function (error, results) {
      if (error) return _this.emit("error", error);

      // Grab initial results
      _this.refresh(true);
    });
  }

  _inherits(LiveSelect, EventEmitter);

  _prototypeProperties(LiveSelect, null, {
    refresh: {
      value: function refresh(condition) {
        var _this = this;
        // Build WHERE clause if not refreshing entire result set
        var values, where;
        if (condition !== true) {
          var valueCount = 0;
          values = _.values(condition);
          where = "WHERE " + _.keys(condition).map(function (key, index) {
            return "" + key + " = $" + (index + 1);
          }).join(" AND ");
        } else {
          values = [];
          where = "";
        }

        this.conn.query("SELECT * FROM " + this.viewName + " " + where, values, function (error, results) {
          if (error) return _this.emit("error", error);
          var rows;
          if (condition !== true) {
            // Do nothing if no change
            if (results.rows.length === 0) return;
            // Partial refresh, copy rows from current data
            rows = _this.data.slice();
            _.forOwn(condition, function (value, key) {
              // Only keep rows that do not match the condition value on key
              rows = rows.filter(function (row) {
                return row[key] !== value;
              });
            });
            // Append new data
            rows = rows.concat(results.rows);
          } else {
            rows = results.rows;
          }

          if (_this.listeners("diff").length !== 0) {
            var diff = [];
            rows.forEach(function (row, index) {
              if (_this.data.length - 1 < index) {
                diff.push(["added", row, index]);
              } else if (JSON.stringify(_this.data[index]) !== JSON.stringify(row)) {
                diff.push(["changed", _this.data[index], row, index]);
              }
            });

            if (_this.data.length > rows.length) {
              for (var i = _this.data.length - 1; i >= rows.length; i--) {
                diff.push(["removed", _this.data[i], i]);
              }
            }
            if (diff.length !== 0) {
              // Output all difference events in a single event
              _this.emit("diff", diff);
            }
          }

          _this.data = rows;
          _this.emit("update", rows);
        });
      },
      writable: true,
      configurable: true
    }
  });

  return LiveSelect;
})(EventEmitter);

module.exports = LiveSelect;