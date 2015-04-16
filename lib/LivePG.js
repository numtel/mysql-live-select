"use strict";

var _classCallCheck = require("babel-runtime/helpers/class-call-check")["default"];

var _inherits = require("babel-runtime/helpers/inherits")["default"];

var _createClass = require("babel-runtime/helpers/create-class")["default"];

var _core = require("babel-runtime/core-js")["default"];

var _regeneratorRuntime = require("babel-runtime/regenerator")["default"];

var EventEmitter = require("events").EventEmitter;
var _ = require("lodash");
var murmurHash = require("murmurhash-js").murmur3;

var common = require("./common");
var SelectHandle = require("./SelectHandle");

/*
 * Duration (ms) to wait to check for new updates when no updates are
 *  available in current frame
 */
var STAGNANT_TIMEOUT = 100;

var LivePG = (function (_EventEmitter) {
  function LivePG(connStr, channel) {
    _classCallCheck(this, LivePG);

    this.connStr = connStr;
    this.channel = channel;
    this.notifyHandle = null;
    this.waitingToUpdate = [];
    this.selectBuffer = {};
    this.allTablesUsed = {};
    this.tablesUsedCache = {};

    this.ready = this.init();
    this.ready["catch"](this._error);
  }

  _inherits(LivePG, _EventEmitter);

  _createClass(LivePG, {
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

              common.performQuery(_this.notifyHandle.client, "LISTEN \"" + _this.channel + "\"")["catch"](_this._error);

              _this.notifyHandle.client.on("notification", function (info) {
                if (info.channel === _this.channel) {
                  try {
                    // See common.createTableTrigger() for payload definition
                    var payload = JSON.parse(info.payload);
                  } catch (error) {
                    return _this._error(new Error("INVALID_NOTIFICATION " + info.payload));
                  }

                  if (payload.table in _this.allTablesUsed) {
                    var _iteratorNormalCompletion = true;
                    var _didIteratorError = false;
                    var _iteratorError = undefined;

                    try {
                      for (var _iterator = _core.$for.getIterator(_this.allTablesUsed[payload.table]), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
                        var queryHash = _step.value;

                        var queryBuffer = _this.selectBuffer[queryHash];
                        if (queryBuffer.triggers
                        // Check for true response from manual trigger
                         && payload.table in queryBuffer.triggers && (payload.op === "UPDATE"
                        // Rows changed in an UPDATE operation must check old and new
                        ? queryBuffer.triggers[payload.table](payload.new_data[0]) || queryBuffer.triggers[payload.table](payload.old_data[0])
                        // Rows changed in INSERT/DELETE operations only check once
                        : queryBuffer.triggers[payload.table](payload.data[0])) || queryBuffer.triggers
                        // No manual trigger for this table, always refresh
                         && !(payload.table in queryBuffer.triggers)
                        // No manual triggers at all, always refresh
                         || !queryBuffer.triggers) {

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
                var _this2 = this;

                if (this.waitingToUpdate.length !== 0) {
                  var queriesToUpdate = _.uniq(this.waitingToUpdate.splice(0, this.waitingToUpdate.length));

                  _core.Promise.all(queriesToUpdate.map(function (queryHash) {
                    return _this2._updateQuery(queryHash);
                  })).then(performNextUpdate)["catch"](this._error);
                } else {
                  // No queries to update, wait for set duration
                  setTimeout(performNextUpdate, STAGNANT_TIMEOUT);
                }
              }).bind(_this);

              performNextUpdate();

            case 7:
            case "end":
              return context$2$0.stop();
          }
        }, null, this);
      }
    },
    select: {
      value: function select(query, params, triggers) {
        // Allow omission of params argument
        if (typeof params === "object" && !(params instanceof Array)) {
          triggers = params;
          params = [];
        } else if (typeof params === "undefined") {
          params = [];
        }

        if (typeof query !== "string") throw new Error("QUERY_STRING_MISSING");
        if (!(params instanceof Array)) throw new Error("PARAMS_ARRAY_MISMATCH");

        var queryHash = murmurHash(JSON.stringify([query, params]));
        var handle = new SelectHandle(this, queryHash);

        // Perform initialization asynchronously
        this._initSelect(query, params, triggers, queryHash, handle)["catch"](this._error);

        return handle;
      }
    },
    _initSelect: {
      value: function _initSelect(query, params, triggers, queryHash, handle) {
        var _this = this;

        var queryBuffer, newBuffer, pgHandle, tablesUsed, _iteratorNormalCompletion, _didIteratorError, _iteratorError, _iterator, _step, table;

        return _regeneratorRuntime.async(function _initSelect$(context$2$0) {
          while (1) switch (context$2$0.prev = context$2$0.next) {
            case 0:
              if (!(queryHash in _this.selectBuffer)) {
                context$2$0.next = 8;
                break;
              }

              queryBuffer = _this.selectBuffer[queryHash];

              queryBuffer.handlers.push(handle);

              context$2$0.next = 5;
              return common.delay();

            case 5:

              // Initial results from cache
              handle.emit("update", { removed: null, moved: null, copied: null, added: queryBuffer.data }, queryBuffer.data);
              context$2$0.next = 54;
              break;

            case 8:
              newBuffer = _this.selectBuffer[queryHash] = {
                query: query,
                params: params,
                triggers: triggers,
                data: [],
                handlers: [handle],
                notifications: []
              };
              context$2$0.next = 11;
              return common.getClient(_this.connStr);

            case 11:
              pgHandle = context$2$0.sent;
              tablesUsed = undefined;

              if (!(queryHash in _this.tablesUsedCache)) {
                context$2$0.next = 17;
                break;
              }

              tablesUsed = _this.tablesUsedCache[queryHash];
              context$2$0.next = 21;
              break;

            case 17:
              context$2$0.next = 19;
              return common.getQueryDetails(pgHandle.client, query);

            case 19:
              tablesUsed = context$2$0.sent;

              _this.tablesUsedCache[queryHash] = tablesUsed;

            case 21:
              _iteratorNormalCompletion = true;
              _didIteratorError = false;
              _iteratorError = undefined;
              context$2$0.prev = 24;
              _iterator = _core.$for.getIterator(tablesUsed);

            case 26:
              if (_iteratorNormalCompletion = (_step = _iterator.next()).done) {
                context$2$0.next = 38;
                break;
              }

              table = _step.value;

              if (table in _this.allTablesUsed) {
                context$2$0.next = 34;
                break;
              }

              _this.allTablesUsed[table] = [queryHash];
              context$2$0.next = 32;
              return common.createTableTrigger(pgHandle.client, table, _this.channel);

            case 32:
              context$2$0.next = 35;
              break;

            case 34:
              if (_this.allTablesUsed[table].indexOf(queryHash) === -1) {
                _this.allTablesUsed[table].push(queryHash);
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
              context$2$0.t0 = context$2$0["catch"](24);
              _didIteratorError = true;
              _iteratorError = context$2$0.t0;

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

              // Retrieve initial results
              _this.waitingToUpdate.push(queryHash);

            case 54:
            case "end":
              return context$2$0.stop();
          }
        }, null, this, [[24, 40, 44, 52], [45,, 47, 51]]);
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
              context$2$0.next = 6;
              return common.getResultSetDiff(pgHandle.client, queryBuffer.data, queryBuffer.query, queryBuffer.params);

            case 6:
              update = context$2$0.sent;

              pgHandle.done();

              if (!(update !== null)) {
                context$2$0.next = 29;
                break;
              }

              queryBuffer.data = update.data;

              _iteratorNormalCompletion = true;
              _didIteratorError = false;
              _iteratorError = undefined;
              context$2$0.prev = 13;
              for (_iterator = _core.$for.getIterator(queryBuffer.handlers); !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
                updateHandler = _step.value;

                updateHandler.emit("update", filterHashProperties(update.diff), filterHashProperties(update.data));
              }
              context$2$0.next = 21;
              break;

            case 17:
              context$2$0.prev = 17;
              context$2$0.t1 = context$2$0["catch"](13);
              _didIteratorError = true;
              _iteratorError = context$2$0.t1;

            case 21:
              context$2$0.prev = 21;
              context$2$0.prev = 22;

              if (!_iteratorNormalCompletion && _iterator["return"]) {
                _iterator["return"]();
              }

            case 24:
              context$2$0.prev = 24;

              if (!_didIteratorError) {
                context$2$0.next = 27;
                break;
              }

              throw _iteratorError;

            case 27:
              return context$2$0.finish(24);

            case 28:
              return context$2$0.finish(21);

            case 29:
            case "end":
              return context$2$0.stop();
          }
        }, null, this, [[13, 17, 21, 29], [22,, 24, 28]]);
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
              _iterator = _core.$for.getIterator(_core.Object.keys(_this.allTablesUsed));

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
              context$2$0.t2 = context$2$0["catch"](7);
              _didIteratorError = true;
              _iteratorError = context$2$0.t2;

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
    },
    _error: {
      value: function _error(reason) {
        this.emit("error", reason);
      }
    }
  });

  return LivePG;
})(EventEmitter);

module.exports = LivePG;
// Expose SelectHandle class so it may be modified by application
module.exports.SelectHandle = SelectHandle;

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
// Give a chance for event listener to be added

// Initialize result set cache