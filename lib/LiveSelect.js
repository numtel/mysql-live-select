"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var _ = require("lodash");
var deep = require("deep-diff");
var EventEmitter = require("events").EventEmitter;

var RowTrigger = require("./RowTrigger");
var querySequence = require("./querySequence");

// Minimum duration in milliseconds between refreshing results
// TODO: determine based on load
// https://git.focus-sis.com/beng/pg-notify-trigger/issues/6
var THROTTLE_INTERVAL = 1000;

var LiveSelect = (function (EventEmitter) {
	function LiveSelect(parent, query, params) {
		var _this = this;
		_classCallCheck(this, LiveSelect);

		this.parent = parent;
		this.query = query;
		this.params = params || [];
		this.hashes = [];
		this.ready = false;
		this.triggers = null;

		this.throttledRefresh = _.debounce(this.refresh, THROTTLE_INTERVAL);

		parent.getQueryTables(this.query).then(function (tables) {
			_this.triggers = tables.map(function (table) {
				return new RowTrigger(parent, table);
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

						// Grab initial results
						_this.refresh();
					}
				});

				trigger.on("change", _this.throttledRefresh.bind(_this));
			});
		}, function (error) {
			return _this.emit("error", error);
		});
	}

	_inherits(LiveSelect, EventEmitter);

	_prototypeProperties(LiveSelect, null, {
		refresh: {
			value: function refresh() {
				var _this = this;
				var _ref = this;
				var parent = _ref.parent;


				var hashQueryPart = function (fullRow) {
					return "\n\t\t\tSELECT\n\t\t\t\tMD5(\n\t\t\t\t\tCAST(tmp.* AS TEXT) ||\n\t\t\t\t\t'" + _.pluck(_this.triggers, "table").join(",") + "'\n\t\t\t\t) AS _hash\n\t\t\t\t" + (fullRow ? ", tmp.*" : "") + "\n\t\t\tFROM\n\t\t\t\ttmp\n\t\t";
				};

				// Run a query to get an updated hash map
				var newHashesQuery = [["\n\t\t\tWITH\n\t\t\t\ttmp AS (" + this.query + ")\n\t\t\tSELECT\n\t\t\t\ttmp2._hash\n\t\t\tFROM (" + hashQueryPart(false) + ") tmp2\n\t\t", this.params]];

				querySequence(parent, newHashesQuery).then(function (result) {
					var freshHashes = _.pluck(result[0].rows, "_hash");
					var diff = deep.diff(_this.hashes, freshHashes);
					var fetch = {};

					// Store the new hash map
					_this.hashes = freshHashes;

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

							if (parent.rowCache.get(tmpChange.oldKey) === null) {
								fetch[tmpChange.oldKey] = true;
							}

							if (parent.rowCache.get(tmpChange.newKey) === null) {
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

							if (parent.rowCache.get(tmpChange.key) === null) {
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
						// Fetch hashes that aren't in the cache
						var newCacheDataQuery = [["\n\t\t\t\t\tWITH\n\t\t\t\t\t\ttmp AS (" + _this.query + ")\n\t\t\t\t\tSELECT\n\t\t\t\t\t\ttmp2.*\n\t\t\t\t\tFROM\n\t\t\t\t\t\t(" + hashQueryPart(true) + ") tmp2\n\t\t\t\t\tWHERE\n\t\t\t\t\t\ttmp2._hash IN ('" + _.keys(fetch).join("', '") + "')\n\t\t\t\t", _this.params]];

						querySequence(parent, newCacheDataQuery).then(function (result) {
							result[0].rows.forEach(function (row) {
								return parent.rowCache.add(row._hash, row);
							});
							_this.update(changes);
						}, function (error) {
							return _this.emit("error", error);
						});
					}
				}, function (error) {
					return _this.emit("error", error);
				});
			},
			writable: true,
			configurable: true
		},
		update: {
			value: function update(changes) {
				var _this = this;
				var _ref = this;
				var parent = _ref.parent;
				var remove = [];

				// Emit an update event with the changes
				var changes = changes.map(function (change) {
					var args = [change.type];

					if (change.type === "added") {
						var row = parent.rowCache.get(change.key);
						args.push(change.index, row);
					} else if (change.type === "changed") {
						var oldRow = parent.rowCache.get(change.oldKey);
						var newRow = parent.rowCache.get(change.newKey);
						args.push(change.index, oldRow, newRow);
						remove.push(change.oldKey);
					} else if (change.type === "removed") {
						var row = parent.rowCache.get(change.key);
						args.push(change.index, row);
						remove.push(change.key);
					}

					if (args[2] === null) {
						return _this.emit("error", new Error("CACHE_MISS: " + (args.length === 3 ? change.key : change.oldKey)));
					}
					if (args.length > 3 && args[3] === null) {
						return _this.emit("error", new Error("CACHE_MISS: " + change.newKey));
					}

					return args;
				});

				remove.forEach(function (key) {
					return parent.rowCache.remove(key);
				});

				this.emit("update", filterHashProperties(changes));
			},
			writable: true,
			configurable: true
		},
		stop: {
			value: function stop() {
				var _ref = this;
				var parent = _ref.parent;
				this.hashes.forEach(function (key) {
					return parent.rowCache.remove(key);
				});
				this.triggers.forEach(function (trigger) {
					return trigger.removeAllListeners();
				});
				this.removeAllListeners();
			},
			writable: true,
			configurable: true
		}
	});

	return LiveSelect;
})(EventEmitter);

module.exports = LiveSelect;

function filterHashProperties(diff) {
	return diff.map(function (event) {
		delete event[2]._hash;
		if (event.length > 3) delete event[3]._hash;
		return event;
	});
}