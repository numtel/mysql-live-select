"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var EventEmitter = require("events").EventEmitter;
var _ = require("lodash");

var getFunctionArgumentNames = require("./getFunctionArgumentNames");

var LiveSelect = (function (EventEmitter) {
  function LiveSelect(parent, query, triggers) {
    var _this = this;
    _classCallCheck(this, LiveSelect);

    var conn = parent.conn;


    this.query = query;
    this.triggers = triggers;
    this.conn = conn;
    this.data = [];
    this.ready = false;

    this.throttledRefresh = _.debounce(this.refresh, 1000, { leading: true });

    this.triggerHandlers = _.map(triggers, function (handler, table) {
      return parent.createTrigger(table, getFunctionArgumentNames(handler));
    });

    this.triggerHandlers.forEach(function (handler) {
      handler.on("change", function (payload) {
        var validator = triggers[handler.table];
        var args = getFunctionArgumentNames(validator);
        if (payload._op === "UPDATE") {
          // Update events contain both old and new values in payload
          // using 'new_' and 'old_' prefixes on the column names
          var argNewVals = args.map(function (arg) {
            return payload["new_" + arg];
          });
          var argOldVals = args.map(function (arg) {
            return payload["old_" + arg];
          });

          if (validator.apply(_this, argNewVals) || validator.apply(_this, argOldVals)) {
            _this.throttledRefresh();
          }
        } else {
          // Insert and Delete events do not have prefixed column names
          var argVals = args.map(function (arg) {
            return payload[arg];
          });
          if (validator.apply(_this, argVals)) _this.throttledRefresh();
        }
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

    // Grab initial results
    this.refresh();
  }

  _inherits(LiveSelect, EventEmitter);

  _prototypeProperties(LiveSelect, null, {
    refresh: {
      value: function refresh() {
        var _this = this;
        this.conn.query(this.query, function (error, results) {
          if (error) return _this.emit("error", error);
          var rows = results.rows;

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