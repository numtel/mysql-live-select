"use strict";

var _babelHelpers = require("babel-runtime/helpers")["default"];

var _core = require("babel-runtime/core-js")["default"];

var _regeneratorRuntime = require("babel-runtime/regenerator")["default"];

var _ = require("lodash");
var murmurHash = require("murmurhash-js").murmur3;

var common = require("./common");

// Number of milliseconds between refreshes
var THROTTLE_INTERVAL = 100;

var LiveSQL = (function () {
	function LiveSQL(connStr, channel) {
		_babelHelpers.classCallCheck(this, LiveSQL);

		this.connStr = connStr;
		this.channel = channel;
		this.notifyHandle = null;
		this.updateInterval = null;
		this.waitingToUpdate = [];
		this.selectBuffer = {};
		this.tablesUsed = {};

		this.ready = this.init();
	}

	_babelHelpers.prototypeProperties(LiveSQL, null, {
		init: {
			value: function init() {
				var _this = this;

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
								if (info.channel === _this.channel && info.payload in _this.tablesUsed) {
									_this.waitingToUpdate = _.union(_this.waitingToUpdate, _this.tablesUsed[info.payload]);
								}
							});

							_this.updateInterval = setInterval(_this.refresh.bind(_this), THROTTLE_INTERVAL);

						case 7:
						case "end":
							return context$2$0.stop();
					}
				}, null, this);
			},
			writable: true,
			configurable: true
		},
		select: {
			value: function select(query, params, onUpdate) {
				var _this = this;

				var queryHash, queryBuffer, pgHandle, queryTables, _iteratorNormalCompletion, _didIteratorError, _iteratorError, _iterator, _step, table, stop;

				return _regeneratorRuntime.async(function select$(context$2$0) {
					while (1) switch (context$2$0.prev = context$2$0.next) {
						case 0:
							// Allow omission of params argument
							if (typeof params === "function" && typeof onUpdate === "undefined") {
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

							throw new ERROR("PARAMS_ARRAY_MISMATCH");

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

							if (bufferData.length !== 0) {
								// Initial results from cache
								onUpdate({ removed: null, moved: null, added: queryBuffer.data }, queryBuffer.data);
							}
							context$2$0.next = 56;
							break;

						case 14:
							// Initialize result set cache
							_this.selectBuffer[queryHash] = {
								data: [],
								query: query,
								params: params,
								handlers: [onUpdate]
							};

							context$2$0.next = 17;
							return common.getClient(_this.connStr);

						case 17:
							pgHandle = context$2$0.sent;
							context$2$0.next = 20;
							return common.getQueryTables(pgHandle.client, query);

						case 20:
							queryTables = context$2$0.sent;
							_iteratorNormalCompletion = true;
							_didIteratorError = false;
							_iteratorError = undefined;
							context$2$0.prev = 24;
							_iterator = _core.$for.getIterator(queryTables);

						case 26:
							if (_iteratorNormalCompletion = (_step = _iterator.next()).done) {
								context$2$0.next = 38;
								break;
							}

							table = _step.value;

							if (table in _this.tablesUsed) {
								context$2$0.next = 34;
								break;
							}

							_this.tablesUsed[table] = [queryHash];
							context$2$0.next = 32;
							return common.createTableTrigger(pgHandle.client, table, _this.channel);

						case 32:
							context$2$0.next = 35;
							break;

						case 34:
							if (_this.tablesUsed[table].indexOf(queryHash) === -1) {
								_this.tablesUsed[table].push(queryHash);
							}

						case 35:
							_iteratorNormalCompletion = true;
							context$2$0.next = 26;
							break;

						case 38:
							context$2$0.next = 44;
							break;

						case 40:
							context$2$0.prev = 40;
							context$2$0.t1 = context$2$0["catch"](24);
							_didIteratorError = true;
							_iteratorError = context$2$0.t1;

						case 44:
							context$2$0.prev = 44;
							context$2$0.prev = 45;

							if (!_iteratorNormalCompletion && _iterator["return"]) {
								_iterator["return"]();
							}

						case 47:
							context$2$0.prev = 47;

							if (!_didIteratorError) {
								context$2$0.next = 50;
								break;
							}

							throw _iteratorError;

						case 50:
							return context$2$0.finish(47);

						case 51:
							return context$2$0.finish(44);

						case 52:

							pgHandle.done();
							pgHandle = null;
							queryTables = null;

							_this.waitingToUpdate.push(queryHash);

						case 56:
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

											stop = null;
											queryHash = null;

										case 27:
										case "end":
											return context$3$0.stop();
									}
								}, null, this, [[9, 13, 17, 25], [18,, 20, 24]]);
							}).bind(_this);

							return context$2$0.abrupt("return", { stop: stop });

						case 58:
						case "end":
							return context$2$0.stop();
					}
				}, null, this, [[24, 40, 44, 52], [45,, 47, 51]]);
			},
			writable: true,
			configurable: true
		},
		refresh: {
			value: function refresh() {
				var _this = this;

				var updateCount, queriesToUpdate, pgHandle, _iteratorNormalCompletion, _didIteratorError, _iteratorError, _iterator, _step, queryHash, queryBuffer, diff, _iteratorNormalCompletion2, _didIteratorError2, _iteratorError2, _iterator2, _step2, updateHandler;

				return _regeneratorRuntime.async(function refresh$(context$2$0) {
					while (1) switch (context$2$0.prev = context$2$0.next) {
						case 0:
							updateCount = _this.waitingToUpdate.length;

							if (!(updateCount === 0)) {
								context$2$0.next = 3;
								break;
							}

							return context$2$0.abrupt("return");

						case 3:
							queriesToUpdate = _.uniq(_this.waitingToUpdate.splice(0, updateCount));
							context$2$0.next = 6;
							return common.getClient(_this.connStr);

						case 6:
							pgHandle = context$2$0.sent;
							_iteratorNormalCompletion = true;
							_didIteratorError = false;
							_iteratorError = undefined;
							context$2$0.prev = 10;
							_iterator = _core.$for.getIterator(queriesToUpdate);

						case 12:
							if (_iteratorNormalCompletion = (_step = _iterator.next()).done) {
								context$2$0.next = 41;
								break;
							}

							queryHash = _step.value;
							queryBuffer = _this.selectBuffer[queryHash];
							context$2$0.next = 17;
							return common.getResultSetDiff(pgHandle.client, queryBuffer.data, queryBuffer.query, queryBuffer.params);

						case 17:
							diff = context$2$0.sent;

							queryBuffer.data = common.applyDiff(queryBuffer.data, diff);

							_iteratorNormalCompletion2 = true;
							_didIteratorError2 = false;
							_iteratorError2 = undefined;
							context$2$0.prev = 22;
							for (_iterator2 = _core.$for.getIterator(queryBuffer.handlers); !(_iteratorNormalCompletion2 = (_step2 = _iterator2.next()).done); _iteratorNormalCompletion2 = true) {
								updateHandler = _step2.value;

								updateHandler(diff, queryBuffer.data);
							}
							context$2$0.next = 30;
							break;

						case 26:
							context$2$0.prev = 26;
							context$2$0.t2 = context$2$0["catch"](22);
							_didIteratorError2 = true;
							_iteratorError2 = context$2$0.t2;

						case 30:
							context$2$0.prev = 30;
							context$2$0.prev = 31;

							if (!_iteratorNormalCompletion2 && _iterator2["return"]) {
								_iterator2["return"]();
							}

						case 33:
							context$2$0.prev = 33;

							if (!_didIteratorError2) {
								context$2$0.next = 36;
								break;
							}

							throw _iteratorError2;

						case 36:
							return context$2$0.finish(33);

						case 37:
							return context$2$0.finish(30);

						case 38:
							_iteratorNormalCompletion = true;
							context$2$0.next = 12;
							break;

						case 41:
							context$2$0.next = 47;
							break;

						case 43:
							context$2$0.prev = 43;
							context$2$0.t3 = context$2$0["catch"](10);
							_didIteratorError = true;
							_iteratorError = context$2$0.t3;

						case 47:
							context$2$0.prev = 47;
							context$2$0.prev = 48;

							if (!_iteratorNormalCompletion && _iterator["return"]) {
								_iterator["return"]();
							}

						case 50:
							context$2$0.prev = 50;

							if (!_didIteratorError) {
								context$2$0.next = 53;
								break;
							}

							throw _iteratorError;

						case 53:
							return context$2$0.finish(50);

						case 54:
							return context$2$0.finish(47);

						case 55:

							pgHandle.done();
							pgHandle = null;

							updateCount = null;
							queriesToUpdate = null;

						case 59:
						case "end":
							return context$2$0.stop();
					}
				}, null, this, [[10, 43, 47, 55], [22, 26, 30, 38], [31,, 33, 37], [48,, 50, 54]]);
			},
			writable: true,
			configurable: true
		},
		cleanup: {
			value: function cleanup() {
				var _this = this;

				var pgHandle, _iteratorNormalCompletion, _didIteratorError, _iteratorError, _iterator, _step, table;

				return _regeneratorRuntime.async(function cleanup$(context$2$0) {
					while (1) switch (context$2$0.prev = context$2$0.next) {
						case 0:
							_this.notifyHandle.done();
							_this.notifyHandle = null;

							clearInterval(_this.updateInterval);
							_this.updateInterval = null;

							context$2$0.next = 6;
							return common.getClient(_this.connStr);

						case 6:
							pgHandle = context$2$0.sent;
							_iteratorNormalCompletion = true;
							_didIteratorError = false;
							_iteratorError = undefined;
							context$2$0.prev = 10;
							_iterator = _core.$for.getIterator(_core.Object.keys(_this.tablesUsed));

						case 12:
							if (_iteratorNormalCompletion = (_step = _iterator.next()).done) {
								context$2$0.next = 19;
								break;
							}

							table = _step.value;
							context$2$0.next = 16;
							return common.dropTableTrigger(pgHandle.client, table, _this.channel);

						case 16:
							_iteratorNormalCompletion = true;
							context$2$0.next = 12;
							break;

						case 19:
							context$2$0.next = 25;
							break;

						case 21:
							context$2$0.prev = 21;
							context$2$0.t4 = context$2$0["catch"](10);
							_didIteratorError = true;
							_iteratorError = context$2$0.t4;

						case 25:
							context$2$0.prev = 25;
							context$2$0.prev = 26;

							if (!_iteratorNormalCompletion && _iterator["return"]) {
								_iterator["return"]();
							}

						case 28:
							context$2$0.prev = 28;

							if (!_didIteratorError) {
								context$2$0.next = 31;
								break;
							}

							throw _iteratorError;

						case 31:
							return context$2$0.finish(28);

						case 32:
							return context$2$0.finish(25);

						case 33:

							pgHandle.done();
							pgHandle = null;

						case 35:
						case "end":
							return context$2$0.stop();
					}
				}, null, this, [[10, 21, 25, 33], [26,, 28, 32]]);
			},
			writable: true,
			configurable: true
		}
	});

	return LiveSQL;
})();

module.exports = LiveSQL;