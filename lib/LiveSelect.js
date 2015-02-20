"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var _ = require("lodash");
var deep = require("deep-diff");
var EventEmitter = require("events").EventEmitter;

var murmurHash = require("murmurhash-js").murmur3;
var querySequence = require("./querySequence");
var RowCache = require("./RowCache");

var cachedQueryTables = {};
var cache = new RowCache();

// Minimum duration in milliseconds between refreshing results
// TODO: determine based on load
//  https://git.focus-sis.com/beng/pg-notify-trigger/issues/6
var THROTTLE_INTERVAL = 1000;

var LiveSelect = (function (EventEmitter) {
	function LiveSelect(parent, query, params) {
		var _this = this;
		_classCallCheck(this, LiveSelect);

		var connect = parent.connect;
		var channel = parent.channel;


		this.query = query;
		this.params = params || [];
		this.connect = connect;
		this.data = [];
		this.hashes = [];
		this.ready = false;

		// throttledRefresh method buffers
		this.throttledRefresh = _.debounce(this.refresh, THROTTLE_INTERVAL);

		this.connect(function (error, client, done) {
			if (error) return _this.emit("error", error);

			getQueryTables(client, _this.query, function (error, tables) {
				if (error) return _this.emit("error", error);

				_this.triggers = tables.map(function (table) {
					return parent.createTrigger(table);
				});

				_this.triggers.forEach(function (trigger) {
					trigger.on("ready", function () {
						// Check if all handlers are ready
						var pending = _this.triggers.filter(function (trigger) {
							return !trigger.ready;
						});

						if (pending.length === 0) {
							_this.ready = true;
							_this.emit("ready");
						}

						trigger.on("change", _this.throttledRefresh.bind(_this));
					});
				});

				done();
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
				// Run a query to get an updated hash map
				var sql = "\n\t\t\tWITH\n\t\t\t\ttmp AS (" + this.query + ")\n\t\t\tSELECT\n\t\t\t\ttmp2._hash\n\t\t\tFROM\n\t\t\t\t(\n\t\t\t\t\tSELECT\n\t\t\t\t\t\tMD5(CAST(tmp.* AS TEXT)) AS _hash\n\t\t\t\t\tFROM\n\t\t\t\t\t\ttmp\n\t\t\t\t) tmp2\n\t\t";

				this.connect(function (error, client, done) {
					if (error) return _this.emit("error", error);

					client.query(sql, _this.params, function (error, result) {
						if (error) return _this.emit("error", error);

						done();

						var hashes = _.pluck(result.rows, "_hash");
						var diff = deep.diff(_this.hashes, hashes);
						var fetch = {};

						// If nothing has changed, stop here
						if (!diff || !diff.length) {
							return;
						}

						// Build a list of changes and hashes to fetch
						var changes = diff.map(function (change) {
							var tmpChange = {};

							if (change.kind === "E") {
								_.extend(tmpChange, {
									type: "changed",
									index: change.path.pop(),
									oldKey: change.lhs,
									newKey: change.rhs
								});

								if (!cache.get(tmpChange.oldKey)) {
									fetch[tmpChange.oldKey] = true;
								}

								if (!cache.get(tmpChange.newKey)) {
									fetch[tmpChange.newKey] = true;
								}
							} else if (change.kind === "A") {
								_.extend(tmpChange, {
									index: change.index
								});

								if (change.item.kind === "N") {
									tmpChange.type = "added";
									tmpChange.key = change.item.rhs;
								} else {
									tmpChange.type = "removed";
									tmpChange.key = change.item.lhs;
								}

								if (!cache.get(tmpChange.key)) {
									fetch[tmpChange.key] = true;
								}
							} else {
								throw new Error("Unrecognized change: " + JSON.stringify(change));
							}

							return tmpChange;
						});

						if (_.isEmpty(fetch)) {
							_this.update(changes);
						} else {
							var sql = "\n\t\t\t\t\t\tWITH\n\t\t\t\t\t\t\ttmp AS (" + _this.query + ")\n\t\t\t\t\t\tSELECT\n\t\t\t\t\t\t\ttmp2.*\n\t\t\t\t\t\tFROM\n\t\t\t\t\t\t\t(\n\t\t\t\t\t\t\t\tSELECT\n\t\t\t\t\t\t\t\t\tMD5(CAST(tmp.* AS TEXT)) AS _hash,\n\t\t\t\t\t\t\t\t\ttmp.*\n\t\t\t\t\t\t\t\tFROM\n\t\t\t\t\t\t\t\t\ttmp\n\t\t\t\t\t\t\t) tmp2\n\t\t\t\t\t\tWHERE\n\t\t\t\t\t\t\ttmp2._hash IN ('" + _.keys(fetch).join("', '") + "')\n\t\t\t\t\t";

							// Fetch hashes that aren't in the cache
							client.query(sql, _this.params, function (error, result) {
								if (error) return _this.emit("error", error);

								result.rows.forEach(function (row) {
									return cache.add(row._hash, row);
								});
								_this.update(changes);
							});

							// Store the current hash map
							_this.hashes = hashes;
						}
					});
				});
			},
			writable: true,
			configurable: true
		},
		update: {
			value: function update(changes) {
				var _this = this;
				var remove = [];

				// Emit an update event with the changes
				var changes = changes.map(function (change) {
					var args = [change.type];

					if (change.type === "added") {
						var row = cache.get(change.key);

						if (!row) {
							return _this.emit("error", new Error("Failed to retrieve row from cache."));
						}

						args.push(change.index, row);
					} else if (change.type === "changed") {
						var oldRow = cache.get(change.oldKey);
						var newRow = cache.get(change.newKey);

						if (!oldRow || !newRow) {
							return _this.emit("error", new Error("Failed to retrieve row from cache."));
						}

						args.push(change.index, oldRow, newRow);
						remove.push(change.oldKey);
					} else if (change.type === "removed") {
						var row = cache.get(change.key);

						if (!row) {
							return _this.emit("error", new Error("Failed to retrieve row from cache."));
						}

						args.push(change.index, row);
						remove.push(change.key);
					}

					return args;
				});

				remove.forEach(function (key) {
					return cache.remove(key);
				});

				this.emit("update", changes);
			},
			writable: true,
			configurable: true
		},
		stop: {
			value: function stop() {
				this.removeAllListeners();
			},
			writable: true,
			configurable: true
		}
	});

	return LiveSelect;
})(EventEmitter);

function getQueryTables(client, query, callback) {
	var queryHash = murmurHash(query);

	// If this query was cached before, reuse it
	if (cachedQueryTables[queryHash]) {
		return callback(null, cachedQueryTables[queryHash]);
	}

	// Replace all parameter values with NULL
	var tmpQuery = query.replace(/\$\d/g, "NULL");
	var tmpName = "tmp_view_" + queryHash;

	var sql = ["CREATE OR REPLACE TEMP VIEW " + tmpName + " AS (" + tmpQuery + ")", ["SELECT DISTINCT vc.table_name\n\t\t\tFROM information_schema.view_column_usage vc\n\t\t\tWHERE view_name = $1", [tmpName]]];

	querySequence(client, sql, function (error, result) {
		if (error) return callback(error);

		var tables = result[1].rows.map(function (row) {
			return row.table_name;
		});

		cachedQueryTables[queryHash] = tables;

		callback(null, tables);
	});
}

module.exports = LiveSelect;