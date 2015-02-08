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
    this.data = null;
    this.ready = false;

    this.throttledRefresh = _.debounce(this.refresh, 1000, { leading: true });

    this.triggerHandlers = _.map(triggers, function (handler, table) {
      return parent.createTrigger(table, getFunctionArgumentNames(handler));
    });

    this.triggerHandlers.forEach(function (handler) {
      // TODO: Fix so that if both UPDATE NEW and UPDATE OLD triggers match,
      //        the results are only updated one time
      handler.on("change", function (payload) {
        var validator = triggers[handler.table];
        var args = getFunctionArgumentNames(validator);
        var argVals = args.map(function (arg) {
          return payload[arg];
        });
        if (validator.apply(_this, argVals)) _this.throttledRefresh();
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
          _this.data = results.rows;
          _this.emit("update", results.rows);
        });
      },
      writable: true,
      configurable: true
    }
  });

  return LiveSelect;
})(EventEmitter);

module.exports = LiveSelect;