"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var EventEmitter = require("events").EventEmitter;
var _ = require("lodash");

var querySequence = require("./querySequence");

var queue = [],
    queueBusy = false;

var RowTrigger = (function (EventEmitter) {
  function RowTrigger(parent, table) {
    var _this = this;
    _classCallCheck(this, RowTrigger);

    this.table = table;
    this.ready = false;

    parent.on("change:" + table, this.forwardNotification.bind(this));

    if (parent.triggerTables.indexOf(table) === -1) {
      parent.triggerTables.push(table);

      // Create the trigger for this table on this channel
      var channel = parent.channel;
      var triggerName = "" + channel + "_" + table;

      console.log("new 1");
      parent.connect(function (error, client, done) {
        if (error) return _this.emit("error", error);

        var sql = ["CREATE OR REPLACE FUNCTION " + triggerName + "() RETURNS trigger AS $$\n            DECLARE\n              row_data RECORD;\n            BEGIN\n              PERFORM pg_notify('" + channel + "', '" + table + "');\n              RETURN NULL;\n            END;\n          $$ LANGUAGE plpgsql", "DROP TRIGGER IF EXISTS \"" + triggerName + "\"\n            ON \"" + table + "\"", "CREATE TRIGGER \"" + triggerName + "\"\n            AFTER INSERT OR UPDATE OR DELETE ON \"" + table + "\"\n            FOR EACH ROW EXECUTE PROCEDURE " + triggerName + "()"];

        querySequence(client, sql, function (error, results) {
          if (error) return _this.emit("error", error);

          _this.ready = true;
          _this.emit("ready");
          console.log("done 1");
          done();
        });
      });
    } else {
      // Triggers already in place
      this.ready = true;
      this.emit("ready");
    }
  }

  _inherits(RowTrigger, EventEmitter);

  _prototypeProperties(RowTrigger, null, {
    forwardNotification: {
      value: function forwardNotification() {
        this.emit("change");
      },
      writable: true,
      configurable: true
    }
  });

  return RowTrigger;
})(EventEmitter);

module.exports = RowTrigger;