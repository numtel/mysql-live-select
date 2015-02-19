"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var EventEmitter = require("events").EventEmitter;
var _ = require("lodash");
var deep = require("deep-diff");

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
            if (_this.triggers.filter(function (trigger) {
              return !trigger.ready;
            }).length === 0) {
              _this.ready = true;
              _this.emit("ready");
            }
          });

          trigger.on("change", _this.throttledRefresh.bind(_this));
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
        var sql = "\n      WITH tmp AS (" + this.query + ")\n      SELECT\n        tmp2._hash\n      FROM\n        (\n          SELECT\n            MD5(CAST(tmp.* AS TEXT)) AS _hash\n          FROM\n            tmp\n        ) tmp2\n    ";

        this.connect(function (error, client, done) {
          if (error) return _this.emit("error", error);

          client.query(sql, _this.params, function (error, result) {
            if (error) return _this.emit("error", error);

            var hashes = _.pluck(result.rows, "_hash");
            var diff = deep.diff(_this.hashes, hashes);
            var fetch = {};

            // If nothing has changed, stop here
            if (!diff) {
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
              var sql = "\n            WITH tmp AS (" + _this.query + ")\n            SELECT\n              tmp2.*\n            FROM\n              (\n                SELECT\n                  MD5(CAST(tmp.* AS TEXT)) AS _hash,\n                  tmp.*\n                FROM\n                  tmp\n              ) tmp2\n            WHERE\n              tmp2._hash IN ('" + _.keys(fetch).join("', '") + "')\n          ";

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

            done();
          });
        });
      },
      writable: true,
      configurable: true
    },
    update: {
      value: function update(changes) {
        // console.log('UPDATE', changes);
        var remove = [];

        // Emit an update event with the changes
        var changes = changes.map(function (change) {
          var args = [change.type];

          if (change.type === "added") {
            var row = cache.get(change.key);

            args.push(change.index, row);
          } else if (change.type === "changed") {
            var oldRow = cache.get(change.oldKey);
            var newRow = cache.get(change.newKey);

            args.push(change.index, oldRow, newRow);
            remove.push(change.oldKey);
          } else if (change.type === "removed") {
            var row = cache.get(change.key);
            args.push(change.index, row);
            remove.push(change.key);
          }

          return args;
        }).filter(function (change) {
          return (
            // Filter cache misses
            change[2] !== null
          );
        });

        // console.log('CHANGES', changes);

        changes.length > 0 && this.emit("update", changes);

        remove.forEach(function (key) {
          return cache.remove(key);
        });
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

  var tmpName = "tmp_view_" + queryHash;
  // Replace all parameter values with NULL
  var tmpQuery = query.replace(/\$\d/g, "NULL");

  querySequence(client, ["CREATE OR REPLACE TEMP VIEW " + tmpName + " AS (" + tmpQuery + ")", ["SELECT DISTINCT vc.table_name\n      FROM information_schema.view_column_usage vc\n      WHERE view_name = $1", [tmpName]]], function (error, result) {
    if (error) return callback(error);

    var tables = cachedQueryTables[queryHash] = result[1].rows.map(function (row) {
      return row.table_name;
    });

    callback(null, tables);
  });
}

module.exports = LiveSelect;