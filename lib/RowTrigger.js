"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var _ = require("lodash");
var EventEmitter = require("events").EventEmitter;

var querySequence = require("./querySequence");
var triggerPromises = {};

var RowTrigger = (function (EventEmitter) {
	function RowTrigger(parent, table) {
		var _this = this;
		_classCallCheck(this, RowTrigger);

		this.table = table;
		this.ready = false;

		parent.on("change:" + table, this.forwardNotification.bind(this));

		if (!triggerPromises[table]) {
			// Create the trigger for this table on this channel
			var channel = parent.channel;
			var triggerName = "" + channel + "_" + table;

			triggerPromises[table] = new Promise(function (resolve, reject) {
				parent.connect(function (error, client, done) {
					if (error) return _this.emit("error", error);

					var sql = ["CREATE OR REPLACE FUNCTION " + triggerName + "() RETURNS trigger AS $$\n\t\t\t\t\t\t\tDECLARE\n\t\t\t\t\t\t\t\trow_data RECORD;\n\t\t\t\t\t\t\tBEGIN\n\t\t\t\t\t\t\t\tPERFORM pg_notify('" + channel + "', '" + table + "');\n\t\t\t\t\t\t\t\tRETURN NULL;\n\t\t\t\t\t\t\tEND;\n\t\t\t\t\t\t$$ LANGUAGE plpgsql", "DROP TRIGGER IF EXISTS \"" + triggerName + "\"\n\t\t\t\t\t\t\tON \"" + table + "\"", "CREATE TRIGGER \"" + triggerName + "\"\n\t\t\t\t\t\t\tAFTER INSERT OR UPDATE OR DELETE ON \"" + table + "\"\n\t\t\t\t\t\t\tFOR EACH ROW EXECUTE PROCEDURE " + triggerName + "()"];

					querySequence(client, sql, function (error, results) {
						if (error) return reject(error);

						done();
						parent.triggerTables.push(table);
						resolve();
					});
				});
			});
		}

		triggerPromises[table].then(function () {
			_this.ready = true;
			_this.emit("ready");
		}, function (error) {
			_this.emit("error", error);
		});
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