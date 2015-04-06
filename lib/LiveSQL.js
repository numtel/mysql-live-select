"use strict";

var _classCallCheck = require("babel-runtime/helpers/class-call-check")["default"];

var _inherits = require("babel-runtime/helpers/inherits")["default"];

var _createClass = require("babel-runtime/helpers/create-class")["default"];

var _core = require("babel-runtime/core-js")["default"];

var _regeneratorRuntime = require("babel-runtime/regenerator")["default"];

var EventEmitter = require("events").EventEmitter;
var _ = require("lodash");
var murmurHash = require("murmurhash-js").murmur3;
var sqlParser = require("sql-parser");

var common = require("./common");
var RateCounter = require("./RateCounter");

/*
 * Global flag to disable simple query optimization
 * Simple queries are those which select from only one table without any
 *  aggregate functions, OFFSET, or GROUP BY used
 * When enabled, these queries will keep the result set current without
 *  repeatedly executing the query on each change
 * TODO Notification payload pagination at 8000 bytes
 */
var ENABLE_SIMPLE_QUERIES = true;

/*
 * Calculate the duration between refreshing result sets
 * As the rate increases, the duration increases to compensate for the time
 *  needed to run the queries
 * TODO Could this be estimated differently?
 *  -> Why not wait for current batch to finish, the grab new batch?
 * @param Integer rate Number of queries to refresh per second
 * @return Integer Timeout duration (ms)
 */
var calcInterval = function (rate) {
	return 100 + 45 * rate;
};

var LiveSQL = (function (_EventEmitter) {
	function LiveSQL(connStr, channel) {
		_classCallCheck(this, LiveSQL);

		this.connStr = connStr;
		this.channel = channel;
		this.notifyHandle = null;
		this.waitingToUpdate = [];
		this.selectBuffer = {};
		this.tablesUsed = {};
		this.queryDetailsCache = {};
		this.refreshRate = new RateCounter();
		// XXX Extra stats for debugging load test
		this.refreshCount = 0;
		this.notifyCount = 0;

		this.ready = this.init();
	}

	_inherits(LiveSQL, _EventEmitter);

	_createClass(LiveSQL, {
		init: {
			value: function init() {
				var _this = this;

				var performNextUpdate;
				return _regeneratorRuntime.async(function init$(context$2$0) {
					while (1) switch (context$2$0.prev = context$2$0.next) {
						case 0:
							context$2$0.next = 2;
							return common.getClient(_this.connStr);

						case 2:
							_this.notifyHandle = context$2$0.sent;
							context$2$0.next = 5;
							return common.performQuery(_this.notifyHandle.client, "LISTEN \"" + _this.channel + "\"");

						case 5:

							_this.notifyHandle.client.on("notification", function (info) {
								if (info.channel === _this.channel) {
									// XXX this.notifyCount is only used for debugging the load test
									_this.notifyCount++;

									try {
										// See common.createTableTrigger() for payload definition
										var payload = JSON.parse(info.payload);
									} catch (error) {
										return _this.emit("error", new Error("INVALID_NOTIFICATION " + info.payload));
									}

									if (payload.table in _this.tablesUsed) {
										var _iteratorNormalCompletion = true;
										var _didIteratorError = false;
										var _iteratorError = undefined;

										try {
											for (var _iterator = _core.$for.getIterator(_this.tablesUsed[payload.table]), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
												var queryHash = _step.value;

												var queryBuffer = _this.selectBuffer[queryHash];
												if (queryBuffer.triggers
												// Check for true response from manual trigger
												 && payload.table in queryBuffer.triggers && (payload.op === "UPDATE"
												// Rows changed in an UPDATE operation must check old and new
												? queryBuffer.triggers[payload.table](payload.new_data[0]) || queryBuffer.triggers[payload.table](payload.old_data[0])
												// Rows changed in INSERT/DELETE operations only check once
												: queryBuffer.triggers[payload.table](payload.data[0])) || queryBuffer.triggers
												// No manual trigger for this table
												 && !(payload.table in queryBuffer.triggers) || !queryBuffer.triggers) {

													if (queryBuffer.parsed !== null) {
														queryBuffer.notifications.push(payload);
													}

													_this.waitingToUpdate.push(queryHash);
												}
											}
										} catch (err) {
											_didIteratorError = true;
											_iteratorError = err;
										} finally {
											try {
												if (!_iteratorNormalCompletion && _iterator["return"]) {
													_iterator["return"]();
												}
											} finally {
												if (_didIteratorError) {
													throw _iteratorError;
												}
											}
										}
									}
								}
							});

							performNextUpdate = (function () {
								var queriesToUpdate = _.uniq(_this.waitingToUpdate.splice(0, _this.waitingToUpdate.length));

								// XXX this.refreshCount is only used for debugging the load test
								_this.refreshCount += queriesToUpdate.length;

								_this.refreshRate.inc(queriesToUpdate.length);

								var _iteratorNormalCompletion = true;
								var _didIteratorError = false;
								var _iteratorError = undefined;

								try {
									for (var _iterator = _core.$for.getIterator(queriesToUpdate), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
										var queryHash = _step.value;

										_this._updateQuery(queryHash);
									}
								} catch (err) {
									_didIteratorError = true;
									_iteratorError = err;
								} finally {
									try {
										if (!_iteratorNormalCompletion && _iterator["return"]) {
											_iterator["return"]();
										}
									} finally {
										if (_didIteratorError) {
											throw _iteratorError;
										}
									}
								}

								setTimeout(performNextUpdate, calcInterval(_this.refreshRate.rate));
							}).bind(_this);

							performNextUpdate();

						case 8:
						case "end":
							return context$2$0.stop();
					}
				}, null, this);
			}
		},
		select: {
			value: function select(query, params, onUpdate, triggers) {
				var _this = this;

				var queryHash, queryBuffer, newBuffer, pgHandle, queryDetails, cleanQuery, _iteratorNormalCompletion, _didIteratorError, _iteratorError, _iterator, _step, table, stop;

				return _regeneratorRuntime.async(function select$(context$2$0) {
					while (1) switch (context$2$0.prev = context$2$0.next) {
						case 0:
							// Allow omission of params argument
							if (typeof params === "function" && typeof onUpdate === "undefined") {
								triggers = onUpdate;
								onUpdate = params;
								params = [];
							}

							if (!(typeof query !== "string")) {
								context$2$0.next = 3;
								break;
							}

							throw new Error("QUERY_STRING_MISSING");

						case 3:
							if (params instanceof Array) {
								context$2$0.next = 5;
								break;
							}

							throw new Error("PARAMS_ARRAY_MISMATCH");

						case 5:
							if (!(typeof onUpdate !== "function")) {
								context$2$0.next = 7;
								break;
							}

							throw new Error("UPDATE_FUNCTION_MISSING");

						case 7:
							queryHash = murmurHash(JSON.stringify([query, params]));

							if (!(queryHash in _this.selectBuffer)) {
								context$2$0.next = 14;
								break;
							}

							queryBuffer = _this.selectBuffer[queryHash];

							queryBuffer.handlers.push(onUpdate);

							// Initial results from cache
							onUpdate({ removed: null, moved: null, copied: null, added: queryBuffer.data }, queryBuffer.data);
							context$2$0.next = 61;
							break;

						case 14:
							newBuffer = _this.selectBuffer[queryHash] = {
								query: query,
								params: params,
								triggers: triggers,
								data: [],
								handlers: [onUpdate],
								// Queries that have parsed property are simple and may be updated
								//  without re-running the query
								parsed: null,
								notifications: []
							};
							context$2$0.next = 17;
							return common.getClient(_this.connStr);

						case 17:
							pgHandle = context$2$0.sent;
							queryDetails = undefined;

							if (!(query in _this.queryDetailsCache)) {
								context$2$0.next = 23;
								break;
							}

							queryDetails = _this.queryDetailsCache[query];
							context$2$0.next = 27;
							break;

						case 23:
							context$2$0.next = 25;
							return common.getQueryDetails(pgHandle.client, query);

						case 25:
							queryDetails = context$2$0.sent;

							_this.queryDetailsCache[query] = queryDetails;

						case 27:

							if (ENABLE_SIMPLE_QUERIES && queryDetails.isUpdatable) {
								cleanQuery = query.replace(/\t/g, " ");

								try {
									newBuffer.parsed = sqlParser.parse(cleanQuery);
								} catch (error) {}

								// OFFSET and GROUP BY not supported with simple queries
								if (newBuffer.parsed && (newBuffer.parsed.limit && newBuffer.parsed.limit.offset || newBuffer.parsed.group)) {
									newBuffer.parsed = null;
								}

								// TODO ensure that query selects primary key column
							}

							_iteratorNormalCompletion = true;
							_didIteratorError = false;
							_iteratorError = undefined;
							context$2$0.prev = 31;
							_iterator = _core.$for.getIterator(queryDetails.tablesUsed);

						case 33:
							if (_iteratorNormalCompletion = (_step = _iterator.next()).done) {
								context$2$0.next = 45;
								break;
							}

							table = _step.value;

							if (table in _this.tablesUsed) {
								context$2$0.next = 41;
								break;
							}

							_this.tablesUsed[table] = [queryHash];
							context$2$0.next = 39;
							return common.createTableTrigger(pgHandle.client, table, _this.channel);

						case 39:
							context$2$0.next = 42;
							break;

						case 41:
							if (_this.tablesUsed[table].indexOf(queryHash) === -1) {
								_this.tablesUsed[table].push(queryHash);
							}

						case 42:
							_iteratorNormalCompletion = true;
							context$2$0.next = 33;
							break;

						case 45:
							context$2$0.next = 51;
							break;

						case 47:
							context$2$0.prev = 47;
							context$2$0.t1 = context$2$0["catch"](31);
							_didIteratorError = true;
							_iteratorError = context$2$0.t1;

						case 51:
							context$2$0.prev = 51;
							context$2$0.prev = 52;

							if (!_iteratorNormalCompletion && _iterator["return"]) {
								_iterator["return"]();
							}

						case 54:
							context$2$0.prev = 54;

							if (!_didIteratorError) {
								context$2$0.next = 57;
								break;
							}

							throw _iteratorError;

						case 57:
							return context$2$0.finish(54);

						case 58:
							return context$2$0.finish(51);

						case 59:

							pgHandle.done();

							// Retrieve initial results
							_this.waitingToUpdate.push(queryHash);

						case 61:
							stop = (function callee$2$0() {
								var _this2 = this;

								var queryBuffer, _iteratorNormalCompletion2, _didIteratorError2, _iteratorError2, _iterator2, _step2, table;

								return _regeneratorRuntime.async(function callee$2$0$(context$3$0) {
									while (1) switch (context$3$0.prev = context$3$0.next) {
										case 0:
											queryBuffer = _this2.selectBuffer[queryHash];

											if (!queryBuffer) {
												context$3$0.next = 25;
												break;
											}

											_.pull(queryBuffer.handlers, onUpdate);

											if (!(queryBuffer.handlers.length === 0)) {
												context$3$0.next = 25;
												break;
											}

											// No more query/params like this, remove from buffers
											delete _this2.selectBuffer[queryHash];
											_.pull(_this2.waitingToUpdate, queryHash);

											_iteratorNormalCompletion2 = true;
											_didIteratorError2 = false;
											_iteratorError2 = undefined;
											context$3$0.prev = 9;
											for (_iterator2 = _core.$for.getIterator(_core.Object.keys(_this2.tablesUsed)); !(_iteratorNormalCompletion2 = (_step2 = _iterator2.next()).done); _iteratorNormalCompletion2 = true) {
												table = _step2.value;

												_.pull(_this2.tablesUsed[table], queryHash);
											}
											context$3$0.next = 17;
											break;

										case 13:
											context$3$0.prev = 13;
											context$3$0.t0 = context$3$0["catch"](9);
											_didIteratorError2 = true;
											_iteratorError2 = context$3$0.t0;

										case 17:
											context$3$0.prev = 17;
											context$3$0.prev = 18;

											if (!_iteratorNormalCompletion2 && _iterator2["return"]) {
												_iterator2["return"]();
											}

										case 20:
											context$3$0.prev = 20;

											if (!_didIteratorError2) {
												context$3$0.next = 23;
												break;
											}

											throw _iteratorError2;

										case 23:
											return context$3$0.finish(20);

										case 24:
											return context$3$0.finish(17);

										case 25:
										case "end":
											return context$3$0.stop();
									}
								}, null, this, [[9, 13, 17, 25], [18,, 20, 24]]);
							}).bind(_this);

							return context$2$0.abrupt("return", { stop: stop });

						case 63:
						case "end":
							return context$2$0.stop();
					}
				}, null, this, [[31, 47, 51, 59], [52,, 54, 58]]);
			}
		},
		_updateQuery: {
			value: function _updateQuery(queryHash) {
				var _this = this;

				var pgHandle, queryBuffer, update, _iteratorNormalCompletion, _didIteratorError, _iteratorError, _iterator, _step, updateHandler;

				return _regeneratorRuntime.async(function _updateQuery$(context$2$0) {
					while (1) switch (context$2$0.prev = context$2$0.next) {
						case 0:
							context$2$0.next = 2;
							return common.getClient(_this.connStr);

						case 2:
							pgHandle = context$2$0.sent;
							queryBuffer = _this.selectBuffer[queryHash];
							update = undefined;

							if (!(queryBuffer.parsed !== null
							// Notifications array will be empty for initial results
							 && queryBuffer.notifications.length !== 0)) {
								context$2$0.next = 11;
								break;
							}

							context$2$0.next = 8;
							return common.getDiffFromSupplied(pgHandle.client, queryBuffer.data, queryBuffer.notifications.splice(0, queryBuffer.notifications.length), queryBuffer.query, queryBuffer.parsed, queryBuffer.params);

						case 8:
							update = context$2$0.sent;
							context$2$0.next = 14;
							break;

						case 11:
							context$2$0.next = 13;
							return common.getResultSetDiff(pgHandle.client, queryBuffer.data, queryBuffer.query, queryBuffer.params);

						case 13:
							update = context$2$0.sent;

						case 14:

							pgHandle.done();

							if (!(update !== null)) {
								context$2$0.next = 36;
								break;
							}

							queryBuffer.data = update.data;

							_iteratorNormalCompletion = true;
							_didIteratorError = false;
							_iteratorError = undefined;
							context$2$0.prev = 20;
							for (_iterator = _core.$for.getIterator(queryBuffer.handlers); !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
								updateHandler = _step.value;

								updateHandler(filterHashProperties(update.diff), filterHashProperties(update.data));
							}
							context$2$0.next = 28;
							break;

						case 24:
							context$2$0.prev = 24;
							context$2$0.t2 = context$2$0["catch"](20);
							_didIteratorError = true;
							_iteratorError = context$2$0.t2;

						case 28:
							context$2$0.prev = 28;
							context$2$0.prev = 29;

							if (!_iteratorNormalCompletion && _iterator["return"]) {
								_iterator["return"]();
							}

						case 31:
							context$2$0.prev = 31;

							if (!_didIteratorError) {
								context$2$0.next = 34;
								break;
							}

							throw _iteratorError;

						case 34:
							return context$2$0.finish(31);

						case 35:
							return context$2$0.finish(28);

						case 36:
						case "end":
							return context$2$0.stop();
					}
				}, null, this, [[20, 24, 28, 36], [29,, 31, 35]]);
			}
		},
		cleanup: {
			value: function cleanup() {
				var _this = this;

				var pgHandle, _iteratorNormalCompletion, _didIteratorError, _iteratorError, _iterator, _step, table;

				return _regeneratorRuntime.async(function cleanup$(context$2$0) {
					while (1) switch (context$2$0.prev = context$2$0.next) {
						case 0:
							_this.notifyHandle.done();

							context$2$0.next = 3;
							return common.getClient(_this.connStr);

						case 3:
							pgHandle = context$2$0.sent;
							_iteratorNormalCompletion = true;
							_didIteratorError = false;
							_iteratorError = undefined;
							context$2$0.prev = 7;
							_iterator = _core.$for.getIterator(_core.Object.keys(_this.tablesUsed));

						case 9:
							if (_iteratorNormalCompletion = (_step = _iterator.next()).done) {
								context$2$0.next = 16;
								break;
							}

							table = _step.value;
							context$2$0.next = 13;
							return common.dropTableTrigger(pgHandle.client, table, _this.channel);

						case 13:
							_iteratorNormalCompletion = true;
							context$2$0.next = 9;
							break;

						case 16:
							context$2$0.next = 22;
							break;

						case 18:
							context$2$0.prev = 18;
							context$2$0.t3 = context$2$0["catch"](7);
							_didIteratorError = true;
							_iteratorError = context$2$0.t3;

						case 22:
							context$2$0.prev = 22;
							context$2$0.prev = 23;

							if (!_iteratorNormalCompletion && _iterator["return"]) {
								_iterator["return"]();
							}

						case 25:
							context$2$0.prev = 25;

							if (!_didIteratorError) {
								context$2$0.next = 28;
								break;
							}

							throw _iteratorError;

						case 28:
							return context$2$0.finish(25);

						case 29:
							return context$2$0.finish(22);

						case 30:

							pgHandle.done();

						case 31:
						case "end":
							return context$2$0.stop();
					}
				}, null, this, [[7, 18, 22, 30], [23,, 25, 29]]);
			}
		}
	});

	return LiveSQL;
})(EventEmitter);

module.exports = LiveSQL;

function filterHashProperties(diff) {
	if (diff instanceof Array) {
		return diff.map(function (event) {
			return _.omit(event, "_hash");
		});
	}
	// Otherwise, diff is object with arrays for keys
	_.forOwn(diff, function (rows, key) {
		diff[key] = filterHashProperties(rows);
	});
	return diff;
}
// Initialize neverending loop to refresh active result sets

// Initialize result set cache

// Query parser does not support tab characters

// Not a serious error, fallback to using full refreshing