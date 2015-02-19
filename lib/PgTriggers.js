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
	function PgTriggers(connect, channel) {
		var _this = this;
		_classCallCheck(this, PgTriggers);

		this.connect = connect;
		this.channel = channel;
		this.triggerTables = [];

		this.setMaxListeners(0); // Allow unlimited listeners

		// Reserve one client to listen for notifications
		this.getClient(function (error, client, done) {
			if (error) return _this.emit("error", error);

			client.query("LISTEN \"" + channel + "\"", function (error, result) {
				if (error) throw error;
			});

			client.on("notification", function (info) {
				if (info.channel === channel) {
					_this.emit("change:" + info.payload);
				}
			});
		});
	}

	_inherits(PgTriggers, EventEmitter);

	_prototypeProperties(PgTriggers, null, {
		getClient: {
			value: function getClient(cb) {
				var _this = this;
				if (this.client && this.done) {
					cb(null, this.client, this.done);
				} else {
					this.connect(function (error, client, done) {
						if (error) return _this.emit("error", error);

						_this.client = client;
						_this.done = done;
					});
				}
			},
			writable: true,
			configurable: true
		},
		createTrigger: {
			value: function createTrigger(table) {
				return new RowTrigger(this, table);
			},
			writable: true,
			configurable: true
		},
		select: {
			value: function select(query, params) {
				return new LiveSelect(this, query, params);
			},
			writable: true,
			configurable: true
		},
		cleanup: {
			value: function cleanup(callback) {
				var _this = this;
				var _ref = this;
				var triggerTables = _ref.triggerTables;
				var channel = _ref.channel;


				var queries = [];

				this.getClient(function (error, client, done) {
					if (error) return _this.emit("error", error);

					triggerTables.forEach(function (table) {
						var triggerName = "" + channel + "_" + table;

						queries.push("DROP TRIGGER IF EXISTS " + triggerName + " ON " + table);
						queries.push("DROP FUNCTION IF EXISTS " + triggerName + "()");
					});

					querySequence(client, queries, function (error, result) {
						done();
						callback(error, result);
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