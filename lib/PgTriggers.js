"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var EventEmitter = require("events").EventEmitter;
var _ = require("lodash");

var querySequence = require("./querySequence");
var RowTrigger = require("./RowTrigger");
var LiveSelect = require("./LiveSelect");

var PgTriggers = (function (EventEmitter) {
  function PgTriggers(client, channel) {
    var _this = this;
    _classCallCheck(this, PgTriggers);

    this.client = client;
    this.channel = channel;
    this.stopped = false;
    this.selects = [];

    this.payloadColumnBuffer = {};
    this.setMaxListeners(0); // Allow unlimited listeners

    client.query("LISTEN \"" + channel + "\"", function (error, result) {
      if (error) throw error;
    });

    client.on("notification", function (info) {
      if (info.channel === channel) {
        try {
          var payload = JSON.parse(info.payload);
        } catch (err) {
          return _this.emit("error", new Error("Malformed payload!"));
        }

        _this.emit("change:" + payload._table, payload);
      }
    });
  }

  _inherits(PgTriggers, EventEmitter);

  _prototypeProperties(PgTriggers, null, {
    createTrigger: {
      value: function createTrigger(table, payloadColumns) {
        return new RowTrigger(this, table, payloadColumns);
      },
      writable: true,
      configurable: true
    },
    select: {
      value: function select(query, params) {
        var select = new LiveSelect(this, query, params);
        this.selects.push(select);
        return select;
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

        this.selects.forEach(function (select) {
          return select.stop(function () {
            var stopped = !_this.selects.filter(function (select) {
              return !select.stopped;
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

  return PgTriggers;
})(EventEmitter);

module.exports = PgTriggers;