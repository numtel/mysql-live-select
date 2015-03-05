"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var _ = require("lodash");
var pg = require("pg");
var EventEmitter = require("events").EventEmitter;
var murmurHash = require("murmurhash-js").murmur3;

var LiveSelect = require("./LiveSelect");
var querySequence = require("./querySequence");

// Number of milliseconds between refreshing result sets
var THROTTLE_INTERVAL = 100;

var PgTriggers = (function (EventEmitter) {
	function PgTriggers(connectionString, channel, hashTable) {
		var _this = this;
		_classCallCheck(this, PgTriggers);

		this.connectionString = connectionString;
		this.channel = channel;
		this.triggerTables = {};
		this.notifyClient = null;
		this.notifyClientDone = null;
		this.cachedQueryTables = {};
		this.resultCache = {};
		this.waitingToUpdate = [];
		this.updateInterval = null;

		this.setMaxListeners(0); // Allow unlimited listeners

		this.init = new Promise(function (resolve, reject) {
			// Reserve one client to listen for notifications
			_this.getClient(function (error, client, done) {
				if (error) return _this.emit("error", error);

				_this.notifyClient = client;
				_this.notifyClientDone = done;

				querySequence(client, ["LISTEN \"" + channel + "\""]).then(resolve, function (error) {
					_this.emit("error", error);reject(error);
				});

				client.on("notification", function (info) {
					if (info.channel === channel && info.payload in _this.triggerTables) {
						_this.waitingToUpdate = _.union(_this.waitingToUpdate, _this.triggerTables[info.payload].updateFunctions);
					}
				});

				// Initialize throttled updater
				// TODO also update when at a threshold waitingToUpdate length
				_this.updateInterval = setInterval(_this.refresh.bind(_this), THROTTLE_INTERVAL);
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
				var _this = this;
				var newSelect = new LiveSelect(this, query, params);
				newSelect.init["catch"](function (error) {
					return _this.emit("error", error);
				});
				return newSelect;
			},
			writable: true,
			configurable: true
		},
		registerQueryTriggers: {
			value: function registerQueryTriggers(query, updateFunction) {
				var _this = this;
				var _ref = this;
				var channel = _ref.channel;
				var triggerTables = _ref.triggerTables;
				return new Promise(function (resolve, reject) {
					_this.getQueryTables(query).then(function (tables) {
						tables.forEach(function (table) {
							if (!(table in triggerTables)) {
								// Create the trigger for this table on this channel
								var triggerName = "" + channel + "_" + table;

								triggerTables[table] = querySequence(_this, ["CREATE OR REPLACE FUNCTION " + triggerName + "() RETURNS trigger AS $$\n\t\t\t\t\t\t\t\tBEGIN\n\t\t\t\t\t\t\t\t\tNOTIFY \"" + channel + "\", '" + table + "';\n\t\t\t\t\t\t\t\t\tRETURN NULL;\n\t\t\t\t\t\t\t\tEND;\n\t\t\t\t\t\t\t$$ LANGUAGE plpgsql", "DROP TRIGGER IF EXISTS \"" + triggerName + "\"\n\t\t\t\t\t\t\t\tON \"" + table + "\"", "CREATE TRIGGER \"" + triggerName + "\"\n\t\t\t\t\t\t\t\tAFTER INSERT OR UPDATE OR DELETE ON \"" + table + "\"\n\t\t\t\t\t\t\t\tEXECUTE PROCEDURE " + triggerName + "()"])["catch"](function (error) {
									return _this.emit("error", error);
								});

								triggerTables[table].updateFunctions = [];
							}

							if (triggerTables[table].updateFunctions.indexOf(updateFunction) === -1) {
								triggerTables[table].updateFunctions.push(updateFunction);
							}
						});
						resolve(tables);
					}, reject);
				});
			},
			writable: true,
			configurable: true
		},
		refresh: {
			value: function refresh() {
				var _this = this;
				var updateCount = this.waitingToUpdate.length;
				if (updateCount === 0) return;

				this.waitingToUpdate.splice(0, updateCount).map(function (updateFunction) {
					var cache = _this.resultCache[updateFunction];
					var curHashes, oldHashes, newHashes, addedRows;
					// Run hash and result query in same transaction
					_this.getClient(function (error, client, done) {
						if (error) return _this.emit("error", error);

						client.query("BEGIN", function (error, result) {
							if (error) return _this.emit("error", error);
							getHashes();
						});

						var getHashes = function () {
							client.query("\n\t\t\t\t\t\tWITH res AS (" + cache.query + ")\n\t\t\t\t\t\tSELECT\n\t\t\t\t\t\t\tMD5(CAST(ROW_TO_JSON(res.*) AS TEXT)) AS _hash\n\t\t\t\t\t\tFROM res\n\t\t\t\t\t", cache.params, function (error, result) {
								if (error) return rollback(error);

								curHashes = result.rows.map(function (row) {
									return row._hash;
								});
								oldHashes = cache.data.map(function (row) {
									return row._hash;
								});
								newHashes = curHashes.filter(function (hash) {
									return oldHashes.indexOf(hash) === -1;
								});

								if (newHashes.length) {
									getRows();
								} else {
									commit();
									addedRows = [];
									generateDiff();
								}
							});
						};

						var getRows = function () {
							client.query("\n\t\t\t\t\t\tWITH\n\t\t\t\t\t\t\tres AS (" + cache.query + "),\n\t\t\t\t\t\t\tres2 AS (\n\t\t\t\t\t\t\t\tSELECT\n\t\t\t\t\t\t\t\t\tMD5(CAST(ROW_TO_JSON(res.*) AS TEXT)) AS _hash,\n\t\t\t\t\t\t\t\t\tres.*\n\t\t\t\t\t\t\t\tFROM res)\n\t\t\t\t\t\tSELECT * from res2\n\t\t\t\t\t\tWHERE _hash IN ('" + newHashes.join("','") + "')\n\t\t\t\t\t", cache.params, function (error, result) {
								if (error) return rollback(error);
								// End transaction as soon as possible
								commit();

								addedRows = result.rows.map(function (row, index) {
									row._index = curHashes.indexOf(row._hash) + 1;
									return row;
								});

								generateDiff();
							});
						};

						var generateDiff = function () {
							var removedHashes = oldHashes.map(function (_hash, index) {
								return { _hash: _hash, _index: index + 1 };
							}).filter(function (removed) {
								return curHashes.indexOf(removed._hash) === -1;
							});

							var movedHashes = curHashes.map(function (hash, newIndex) {
								var oldIndex = oldHashes.indexOf(hash);
								if (oldIndex !== -1 && oldIndex !== newIndex) {
									return {
										old_index: oldIndex + 1,
										new_index: newIndex + 1,
										_hash: hash
									};
								}
							}).filter(function (moved) {
								return moved !== undefined;
							});

							var diff = {
								removed: removedHashes.length !== 0 ? removedHashes : null,
								moved: movedHashes.length !== 0 ? movedHashes : null,
								added: addedRows.length !== 0 ? addedRows : null
							};

							if (diff.added === null && diff.moved === null && diff.removed === null) return;

							var rows = cache.data = _this.calcUpdatedResultCache(cache.data, diff);

							console.log("update", updateFunction, diff);
							_this.emit(updateFunction, diff, rows);
						};

						var rollback = function (error) {
							_this.emit("error", error);
							client.query("ROLLBACK", function (error, result) {
								done();
								if (error) return _this.emit("error", error);
							});
						};

						var commit = function () {
							client.query("COMMIT", function (error, result) {
								done();
								console.log("committin", error);
								if (error) return _this.emit("error", error);
							});
						};
					});
				});
			},
			writable: true,
			configurable: true
		},
		calcUpdatedResultCache: {
			value: function calcUpdatedResultCache(oldResults, diff) {
				var newResults = oldResults.slice();

				diff.removed !== null && diff.removed.forEach(function (removed) {
					return newResults[removed._index - 1] = undefined;
				});

				// Deallocate first to ensure no overwrites
				diff.moved !== null && diff.moved.forEach(function (moved) {
					newResults[moved.old_index - 1] = undefined;
				});

				diff.moved !== null && diff.moved.forEach(function (moved) {
					var movingRow = oldResults[moved.old_index - 1];
					movingRow._index = moved.new_index;
					newResults[moved.new_index - 1] = movingRow;
				});

				diff.added !== null && diff.added.forEach(function (added) {
					return newResults[added._index - 1] = added;
				});

				return newResults.filter(function (row) {
					return row !== undefined;
				});
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
				this.removeAllListeners();
				this.updateInterval !== null && clearInterval(this.updateInterval);

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