"use strict";

var _babelHelpers = require("babel-runtime/helpers")["default"];

var _core = require("babel-runtime/core-js")["default"];

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

		_babelHelpers.classCallCheck(this, PgTriggers);

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

		this.init = new _core.Promise(function (resolve, reject) {
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

	_babelHelpers.inherits(PgTriggers, EventEmitter);

	_babelHelpers.prototypeProperties(PgTriggers, null, {
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

				return new _core.Promise(function (resolve, reject) {
					_this.getQueryTables(query).then(function (tables) {
						_core.Promise.all(tables.map(function (table) {
							if (!(table in triggerTables)) {
								// Create the trigger for this table on this channel
								var triggerName = "" + channel + "_" + table;

								triggerTables[table] = querySequence(_this, ["CREATE OR REPLACE FUNCTION " + triggerName + "() RETURNS trigger AS $$\n\t\t\t\t\t\t\t\tBEGIN\n\t\t\t\t\t\t\t\t\tNOTIFY \"" + channel + "\", '" + table + "';\n\t\t\t\t\t\t\t\t\tRETURN NULL;\n\t\t\t\t\t\t\t\tEND;\n\t\t\t\t\t\t\t$$ LANGUAGE plpgsql", "DROP TRIGGER IF EXISTS \"" + triggerName + "\"\n\t\t\t\t\t\t\t\tON \"" + table + "\"", "CREATE TRIGGER \"" + triggerName + "\"\n\t\t\t\t\t\t\t\tAFTER INSERT OR UPDATE OR DELETE ON \"" + table + "\"\n\t\t\t\t\t\t\t\tEXECUTE PROCEDURE " + triggerName + "()"])["catch"](function (error) {
									return _this.emit("error", error);
								});

								triggerTables[table].updateFunctions = [updateFunction];
								return triggerTables[table];
							} else {
								if (triggerTables[table].updateFunctions.indexOf(updateFunction) === -1) {
									triggerTables[table].updateFunctions.push(updateFunction);
								}
								return _core.Promise.resolve();
							}
						})).then(function () {
							resolve(tables);
						}, reject);
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
				if (updateCount === 0) {
					return;
				}this.waitingToUpdate.splice(0, updateCount).map(function (updateFunction) {
					var cache = _this.resultCache[updateFunction];
					var curHashes, oldHashes, newHashes, addedRows;
					oldHashes = cache.data.map(function (row) {
						return row._hash;
					});

					querySequence(_this, [["\n\t\t\t\tWITH\n\t\t\t\t\tres AS (" + cache.query + "),\n\t\t\t\t\tdata AS (\n\t\t\t\t\t\tSELECT\n\t\t\t\t\t\t\tMD5(CAST(ROW_TO_JSON(res.*) AS TEXT)) AS _hash,\n\t\t\t\t\t\t\tres.*\n\t\t\t\t\t\tFROM res),\n\t\t\t\t\tdata2 AS (\n\t\t\t\t\t\tSELECT\n\t\t\t\t\t\t\t1 AS _added,\n\t\t\t\t\t\t\tdata.*\n\t\t\t\t\t\tFROM data\n\t\t\t\t\t\tWHERE _hash NOT IN ('" + oldHashes.join("','") + "'))\n\t\t\t\tSELECT\n\t\t\t\t\tdata2.*,\n\t\t\t\t\tdata._hash AS _hash\n\t\t\t\tFROM data\n\t\t\t\tLEFT JOIN data2\n\t\t\t\t\tON (data._hash = data2._hash)\n\t\t\t", cache.params]]).then(function (result) {
						// 				console.log(result[0].rows);

						curHashes = result[0].rows.map(function (row) {
							return row._hash;
						});
						newHashes = curHashes.filter(function (hash) {
							return oldHashes.indexOf(hash) === -1;
						});

						var curHashes2 = curHashes.slice();
						addedRows = result[0].rows.filter(function (row) {
							return row._added === 1;
						}).map(function (row, index) {
							row._index = curHashes2.indexOf(row._hash) + 1;
							delete row._added;

							// Clear this hash so that duplicate hashes can move forward
							curHashes2[row._index - 1] = undefined;

							return row;
						});

						var movedHashes = curHashes.map(function (hash, newIndex) {
							var oldIndex = oldHashes.indexOf(hash);
							if (oldIndex !== -1 && oldIndex !== newIndex && curHashes[oldIndex] !== hash) {
								return {
									old_index: oldIndex + 1,
									new_index: newIndex + 1,
									_hash: hash
								};
							}
						}).filter(function (moved) {
							return moved !== undefined;
						});

						var removedHashes = oldHashes.map(function (_hash, index) {
							return { _hash: _hash, _index: index + 1 };
						}).filter(function (removed) {
							return curHashes[removed._index - 1] !== removed._hash && movedHashes.filter(function (moved) {
								return moved.new_index === removed._index;
							}).length === 0;
						});

						// Add rows that have already existing hash but in new places
						var copiedHashes = curHashes.map(function (hash, index) {
							var oldHashIndex = oldHashes.indexOf(hash);
							if (oldHashIndex !== -1 && oldHashes[index] !== hash && movedHashes.filter(function (moved) {
								return moved.new_index - 1 === index;
							}).length === 0 && addedRows.filter(function (added) {
								return added._index - 1 === index;
							}).length === 0) {
								return {
									new_index: index + 1,
									orig_index: oldHashIndex + 1
								};
							}
						}).filter(function (copied) {
							return copied !== undefined;
						});

						var diff = {
							removed: removedHashes.length !== 0 ? removedHashes : null,
							moved: movedHashes.length !== 0 ? movedHashes : null,
							copied: copiedHashes.length !== 0 ? copiedHashes : null,
							added: addedRows.length !== 0 ? addedRows : null
						};

						if (diff.added === null && diff.moved === null && diff.copied === null && diff.removed === null) return;

						var rows = cache.data = _this.calcUpdatedResultCache(cache.data, diff);

						_this.emit(updateFunction, diff, rows);
					}, function (error) {
						return _this.emit("error", error);
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

				diff.copied !== null && diff.copied.forEach(function (copied) {
					var copyRow = _.clone(oldResults[copied.orig_index - 1]);
					if (!copyRow) {
						// TODO why do some copied rows not exist in the old data?
						console.log(copied, oldResults.length);
					}
					copyRow._index = copied.new_index;
					newResults[copied.new_index - 1] = copyRow;
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

				return new _core.Promise(function (resolve, reject) {
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