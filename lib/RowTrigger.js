"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var EventEmitter = require("events").EventEmitter;
var _ = require("lodash");

var querySequence = require("./querySequence");

var RowTrigger = (function (EventEmitter) {
  function RowTrigger(parent, table, payloadColumns) {
    var _this = this;
    _classCallCheck(this, RowTrigger);

    this.table = table;
    this.payloadColumns = payloadColumns;
    this.ready = false;

    var payloadColumnBuffer = parent.payloadColumnBuffer;
    var client = parent.client;
    var channel = parent.channel;


    parent.on("change:" + table, this.forwardNotification.bind(this));

    // Merge these columns to the trigger's payload
    if (!(table in payloadColumnBuffer)) {
      payloadColumnBuffer[table] = payloadColumns.slice();
    } else {
      payloadColumns = payloadColumnBuffer[table] = _.union(payloadColumnBuffer[table], payloadColumns);
    }

    // Update the trigger for this table on this channel
    var payloadTpl = "\n      SELECT\n        '" + table + "'  AS _table,\n        TG_OP       AS _op,\n        " + payloadColumns.map(function (col) {
      return "$ROW$." + col;
    }).join(", ") + "\n      INTO row_data;\n    ";
    var payloadNew = payloadTpl.replace(/\$ROW\$/g, "NEW");
    var payloadOld = payloadTpl.replace(/\$ROW\$/g, "OLD");
    var payloadChanged = "\n      SELECT\n        '" + table + "'  AS _table,\n        TG_OP       AS _op,\n        " + payloadColumns.map(function (col) {
      return "NEW." + col + " AS new_" + col;
    }).concat(payloadColumns.map(function (col) {
      return "OLD." + col + " AS old_" + col;
    })).join(", ") + "\n      INTO row_data;\n    ";

    var triggerName = "" + channel + "_" + table;

    querySequence(client, ["CREATE OR REPLACE FUNCTION " + triggerName + "() RETURNS trigger AS $$\n        DECLARE\n          row_data RECORD;\n        BEGIN\n          IF (TG_OP = 'INSERT') THEN\n            " + payloadNew + "\n          ELSIF (TG_OP  = 'DELETE') THEN\n            " + payloadOld + "\n          ELSIF (TG_OP = 'UPDATE') THEN\n            " + payloadChanged + "\n          END IF;\n          PERFORM pg_notify('" + channel + "', row_to_json(row_data)::TEXT);\n          RETURN NULL;\n        END;\n      $$ LANGUAGE plpgsql", "DROP TRIGGER IF EXISTS \"" + triggerName + "\"\n        ON \"" + table + "\"", "CREATE TRIGGER \"" + triggerName + "\"\n        AFTER INSERT OR UPDATE OR DELETE ON \"" + table + "\"\n        FOR EACH ROW EXECUTE PROCEDURE " + triggerName + "()"], function (error, results) {
      if (error) return _this.emit("error", error);
      _this.ready = true;
      _this.emit("ready", results);
    });
  }

  _inherits(RowTrigger, EventEmitter);

  _prototypeProperties(RowTrigger, null, {
    forwardNotification: {
      value: function forwardNotification(payload) {
        this.emit("change", payload);
      },
      writable: true,
      configurable: true
    }
  });

  return RowTrigger;
})(EventEmitter);

module.exports = RowTrigger;