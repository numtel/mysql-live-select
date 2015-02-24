"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var _ = require("lodash");
var pg = require("pg");
var EventEmitter = require("events").EventEmitter;
var murmurHash = require("murmurhash-js").murmur3;

var RowCache = require("./RowCache");
var LiveSelect = require("./LiveSelect");
var querySequence = require("./querySequence");

var PgTriggers = (function (EventEmitter) {
	function PgTriggers(connectionString, channel) {
		var _this = this;
		_classCallCheck(this, PgTriggers);

		this.connectionString = connectionString;
		this.channel = channel;
		this.rowCache = new RowCache();
		this.triggerTables = {};
		this.notifyClient = null;
		this.notifyClientDone = null;
		this.cachedQueryTables = {};

		this.setMaxListeners(0); // Allow unlimited listeners

		// Reserve one client to listen for notifications
		this.getClient(function (error, client, done) {
			if (error) return _this.emit("error", error);

			_this.notifyClient = client;
			_this.notifyClientDone = done;

			client.query("LISTEN \"" + channel + "\"", function (error, result) {
				if (error) this.emit("error", error);
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
				pg.connect(this.connectionString, cb);
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
		getQueryTables: {

			/**
    * Retrieve the tables used in a query
    * @param  String query May contain placeholders as they will be nullified
    * @return Promise
    */
			value: function getQueryTables(query) {
				var _this = this;
				return new Promise(function (resolve, reject) {
					var queryHash = murmurHash(query);

					// If this query was cached before, reuse it
					if (_this.cachedQueryTables[queryHash]) {
						return resolve(_this.cachedQueryTables[queryHash]);
					}

					// Replace all parameter values with NULL
					var tmpQuery = query.replace(/\$\d/g, "NULL");
					var tmpName = "tmp_view_" + queryHash;

					querySequence(_this, ["CREATE OR REPLACE TEMP VIEW " + tmpName + " AS (" + tmpQuery + ")", ["SELECT DISTINCT vc.table_name\n\t\t\t\t\tFROM information_schema.view_column_usage vc\n\t\t\t\t\tWHERE view_name = $1", [tmpName]]]).then(function (result) {
						var tables = result[1].rows.map(function (row) {
							return row.table_name;
						});
						_this.cachedQueryTables[queryHash] = tables;
						resolve(tables);
					}, reject);
				});
			},
			writable: true,
			configurable: true
		},
		cleanup: {

			/**
    * Drop all active triggers and close notification client
    * @param  Function callback Optional (error, result)
    * @return Promise
    */
			value: function cleanup(callback) {
				var _ref = this;
				var triggerTables = _ref.triggerTables;
				var channel = _ref.channel;


				this.notifyClientDone();

				var queries = [];
				_.forOwn(triggerTables, function (tablePromise, table) {
					var triggerName = "" + channel + "_" + table;

					queries.push("DROP TRIGGER IF EXISTS " + triggerName + " ON " + table);
					queries.push("DROP FUNCTION IF EXISTS " + triggerName + "()");
				});

				return querySequence(this, queries, callback);
			},
			writable: true,
			configurable: true
		}
	});

	return PgTriggers;
})(EventEmitter);

module.exports = PgTriggers;