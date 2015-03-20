"use strict";

var _core = require("babel-runtime/core-js")["default"];

var _regeneratorRuntime = require("babel-runtime/regenerator")["default"];

var _ = require("lodash");
var md5 = require("md5");
var pg = require("pg");
var randomString = require("random-strings");

var collectionDiff = require("./collectionDiff");
var matchRows = require("./matchRowsToParsedQuery");

module.exports = exports = {

	/**
  * Obtain a node-postgres client from the connection pool
  * @param  String  connectionString "postgres://user:pass@host/database"
  * @return Promise { client, done() } Call done() to return client to pool!
  */
	getClient: function getClient(connectionString) {
		return new _core.Promise(function (resolve, reject) {
			pg.connect(connectionString, function (error, client, done) {
				if (error) reject(error);else resolve({ client: client, done: done });
			});
		});
	},

	/**
  * Perform a query
  * @param  Object client node-postgres client
  * @param  String query  SQL statement
  * @param  Array  params Optional, values to substitute into query
  *                       (params[0] => '$1'...)
  * @return Promise Array Result set
  */
	performQuery: function performQuery(client, query) {
		var params = arguments[2] === undefined ? [] : arguments[2];

		return new _core.Promise(function (resolve, reject) {
			client.query(query, params, function (error, result) {
				if (error) reject(error);else resolve(result);
			});
		});
	},

	nextTick: function nextTick() {
		return new _core.Promise(function (resolve, reject) {
			return process.nextTick(resolve);
		});
	},

	/**
  * Query information_schema to determine tables used and if updatable
  * @param  Object client node-postgres client
  * @param  String query  SQL statement, params not used
  * @return Promise Array Table names
  */
	getQueryDetails: function getQueryDetails(client, query) {
		var nullifiedQuery, viewName, tablesResult, isUpdatableResult;
		return _regeneratorRuntime.async(function getQueryDetails$(context$1$0) {
			while (1) switch (context$1$0.prev = context$1$0.next) {
				case 0:
					nullifiedQuery = query.replace(/\$\d+/g, "NULL");
					viewName = "tmp_view_" + randomString.alphaLower(10);
					context$1$0.next = 4;
					return exports.performQuery(client, "CREATE OR REPLACE TEMP VIEW " + viewName + " AS (" + nullifiedQuery + ")");

				case 4:
					context$1$0.next = 6;
					return exports.performQuery(client, "SELECT DISTINCT vc.table_name\n\t\t\t\tFROM information_schema.view_column_usage vc\n\t\t\t\tWHERE view_name = $1", [viewName]);

				case 6:
					tablesResult = context$1$0.sent;
					context$1$0.next = 9;
					return exports.performQuery(client, "SELECT is_updatable\n\t\t\t\tFROM information_schema.views\n\t\t\t\tWHERE table_name = $1", [viewName]);

				case 9:
					isUpdatableResult = context$1$0.sent;
					context$1$0.next = 12;
					return exports.performQuery(client, "DROP VIEW " + viewName);

				case 12:
					return context$1$0.abrupt("return", {
						isUpdatable: isUpdatableResult.rows[0].is_updatable === "YES",
						tablesUsed: tablesResult.rows.map(function (row) {
							return row.table_name;
						})
					});

				case 13:
				case "end":
					return context$1$0.stop();
			}
		}, null, this);
	},

	/**
  * Create a trigger to send NOTIFY on any change with payload of table name
  * @param  Object client  node-postgres client
  * @param  String table   Name of table to install trigger
  * @param  String channel NOTIFY channel
  * @return Promise true   Successful
  */
	createTableTrigger: function createTableTrigger(client, table, channel) {
		var triggerName, payloadTpl, payloadNew, payloadOld, payloadChanged;
		return _regeneratorRuntime.async(function createTableTrigger$(context$1$0) {
			while (1) switch (context$1$0.prev = context$1$0.next) {
				case 0:
					triggerName = "" + channel + "_" + table;
					payloadTpl = "\n\t\t\tSELECT\n\t\t\t\t'" + table + "'  AS table,\n\t\t\t\tTG_OP       AS op,\n\t\t\t\tjson_agg($ROW$) AS data\n\t\t\tINTO row_data;\n\t\t";
					payloadNew = payloadTpl.replace(/\$ROW\$/g, "NEW");
					payloadOld = payloadTpl.replace(/\$ROW\$/g, "OLD");
					payloadChanged = "\n\t\t\tSELECT\n\t\t\t\t'" + table + "'  AS table,\n\t\t\t\tTG_OP       AS op,\n\t\t\t\tjson_agg(NEW) AS new_data,\n\t\t\t\tjson_agg(OLD) AS old_data\n\t\t\tINTO row_data;\n\t\t";
					context$1$0.next = 7;
					return exports.performQuery(client, "CREATE OR REPLACE FUNCTION " + triggerName + "() RETURNS trigger AS $$\n\t\t\t\tDECLARE\n          row_data RECORD;\n        BEGIN\n          IF (TG_OP = 'INSERT') THEN\n            " + payloadNew + "\n          ELSIF (TG_OP  = 'DELETE') THEN\n            " + payloadOld + "\n          ELSIF (TG_OP = 'UPDATE') THEN\n            " + payloadChanged + "\n          END IF;\n          PERFORM pg_notify('" + channel + "', row_to_json(row_data)::TEXT);\n          RETURN NULL;\n\t\t\t\tEND;\n\t\t\t$$ LANGUAGE plpgsql");

				case 7:
					context$1$0.next = 9;
					return exports.performQuery(client, "DROP TRIGGER IF EXISTS \"" + triggerName + "\"\n\t\t\t\tON \"" + table + "\"");

				case 9:
					context$1$0.next = 11;
					return exports.performQuery(client, "CREATE TRIGGER \"" + triggerName + "\"\n\t\t\t\tAFTER INSERT OR UPDATE OR DELETE ON \"" + table + "\"\n\t\t\t\tFOR EACH ROW EXECUTE PROCEDURE " + triggerName + "()");

				case 11:
					return context$1$0.abrupt("return", true);

				case 12:
				case "end":
					return context$1$0.stop();
			}
		}, null, this);
	},

	/**
  * Drop matching function and trigger for a table
  * @param  Object client  node-postgres client
  * @param  String table   Name of table to remove trigger
  * @param  String channel NOTIFY channel
  * @return Promise true   Successful
  */
	dropTableTrigger: function dropTableTrigger(client, table, channel) {
		var triggerName;
		return _regeneratorRuntime.async(function dropTableTrigger$(context$1$0) {
			while (1) switch (context$1$0.prev = context$1$0.next) {
				case 0:
					triggerName = "" + channel + "_" + table;
					context$1$0.next = 3;
					return exports.performQuery(client, "DROP TRIGGER IF EXISTS " + triggerName + " ON " + table);

				case 3:
					context$1$0.next = 5;
					return exports.performQuery(client, "DROP FUNCTION IF EXISTS " + triggerName + "()");

				case 5:
					return context$1$0.abrupt("return", true);

				case 6:
				case "end":
					return context$1$0.stop();
			}
		}, null, this);
	},

	/**
  * Using supplied NOTIFY payloads, check which rows match query
  * @param  Object  client        node-postgres client (Used only in fallback)
  * @param  Array   currentData   Last known result set for this query/params
  * @param  Array   notifications Payloads from NOTIFY
  * @param  String  query         SQL SELECT statement
  * @param  String  parsed        Parsed SQL SELECT statement
  * @param  Array   params        Optionally, pass an array of parameters
  * @return Promise Object        Enumeration of differences
  */
	getDiffFromSupplied: function getDiffFromSupplied(client, currentData, notifications, query, parsed, params) {
		var allRows, matched, oldHashes, newData, hasDelete, _iteratorNormalCompletion, _didIteratorError, _iteratorError, _iterator, _step, matchRow, cleanRow, curIndex, sortProps, sortOrders, _iteratorNormalCompletion2, _didIteratorError2, _iteratorError2, _iterator2, _step2, index, diff;

		return _regeneratorRuntime.async(function getDiffFromSupplied$(context$1$0) {
			while (1) switch (context$1$0.prev = context$1$0.next) {
				case 0:
					context$1$0.next = 2;
					return exports.nextTick();

				case 2:
					allRows = flattenNotifications(notifications);
					context$1$0.next = 5;
					return exports.nextTick();

				case 5:
					matched = matchRows(allRows, parsed, params);

					if (!(matched.length === 0)) {
						context$1$0.next = 8;
						break;
					}

					return context$1$0.abrupt("return", null);

				case 8:
					oldHashes = currentData.map(function (row) {
						return row._hash;
					});
					context$1$0.next = 11;
					return exports.nextTick();

				case 11:
					newData = currentData.slice();
					hasDelete = false;
					_iteratorNormalCompletion = true;
					_didIteratorError = false;
					_iteratorError = undefined;
					context$1$0.prev = 16;

					for (_iterator = _core.$for.getIterator(matched); !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
						matchRow = _step.value;
						cleanRow = _.clone(matchRow);

						// All extra fields must be removed for hashing
						delete cleanRow._op;
						delete cleanRow._key;
						delete cleanRow._index;

						cleanRow._hash = md5.digest_s(JSON.stringify(cleanRow));

						curIndex = oldHashes.indexOf(cleanRow._hash);

						if (curIndex !== -1 && (matchRow._op === "DELETE" || matchRow._op === "UPDATE" && matchRow._key === "old_data")) {

							newData[curIndex] = undefined;
							hasDelete = true;
						}

						if (matchRow._op === "INSERT" || matchRow._op === "UPDATE" && matchRow._key === "new_data") {
							cleanRow._added = 1;
							newData.push(cleanRow);
						}
					}

					context$1$0.next = 24;
					break;

				case 20:
					context$1$0.prev = 20;
					context$1$0.t4 = context$1$0["catch"](16);
					_didIteratorError = true;
					_iteratorError = context$1$0.t4;

				case 24:
					context$1$0.prev = 24;
					context$1$0.prev = 25;

					if (!_iteratorNormalCompletion && _iterator["return"]) {
						_iterator["return"]();
					}

				case 27:
					context$1$0.prev = 27;

					if (!_didIteratorError) {
						context$1$0.next = 30;
						break;
					}

					throw _iteratorError;

				case 30:
					return context$1$0.finish(27);

				case 31:
					return context$1$0.finish(24);

				case 32:
					if (!(hasDelete === true && parsed.limit && parsed.limit.value.value === currentData.length)) {
						context$1$0.next = 36;
						break;
					}

					context$1$0.next = 35;
					return exports.getResultSetDiff(client, currentData, query, params);

				case 35:
					return context$1$0.abrupt("return", context$1$0.sent);

				case 36:

					// Clean out deleted rows
					newData = newData.filter(function (row) {
						return row !== undefined;
					});

					// Apply ORDER BY, LIMIT
					// Queries with unsupported clauses (e.g. OFFSET) filtered upstream
					if (parsed.order) {
						sortProps = parsed.order.orderings.map(function (ordering) {
							return ordering.value.value;
						});
						sortOrders = parsed.order.orderings.map(function (ordering) {
							return ordering.direction.toUpperCase() === "ASC";
						});

						newData = _.sortByOrder(newData, sortProps, sortOrders);
					}

					if (parsed.limit) {
						newData = newData.slice(0, parsed.limit.value.value);
					}

					_iteratorNormalCompletion2 = true;
					_didIteratorError2 = false;
					_iteratorError2 = undefined;
					context$1$0.prev = 42;
					// Fix indexes
					for (_iterator2 = _core.$for.getIterator(_.range(newData.length)); !(_iteratorNormalCompletion2 = (_step2 = _iterator2.next()).done); _iteratorNormalCompletion2 = true) {
						index = _step2.value;

						newData[index]._index = index + 1;
					}

					context$1$0.next = 50;
					break;

				case 46:
					context$1$0.prev = 46;
					context$1$0.t5 = context$1$0["catch"](42);
					_didIteratorError2 = true;
					_iteratorError2 = context$1$0.t5;

				case 50:
					context$1$0.prev = 50;
					context$1$0.prev = 51;

					if (!_iteratorNormalCompletion2 && _iterator2["return"]) {
						_iterator2["return"]();
					}

				case 53:
					context$1$0.prev = 53;

					if (!_didIteratorError2) {
						context$1$0.next = 56;
						break;
					}

					throw _iteratorError2;

				case 56:
					return context$1$0.finish(53);

				case 57:
					return context$1$0.finish(50);

				case 58:
					diff = collectionDiff(oldHashes, newData);

					if (!(diff === null)) {
						context$1$0.next = 61;
						break;
					}

					return context$1$0.abrupt("return", null);

				case 61:
					return context$1$0.abrupt("return", { diff: diff, data: newData });

				case 62:
				case "end":
					return context$1$0.stop();
			}
		}, null, this, [[16, 20, 24, 32], [25,, 27, 31], [42, 46, 50, 58], [51,, 53, 57]]);
	},

	/**
  * Perform SELECT query, obtaining difference in result set
  * @param  Object  client      node-postgres client
  * @param  Array   currentData Last known result set for this query/params
  * @param  String  query       SQL SELECT statement
  * @param  Array   params      Optionally, pass an array of parameters
  * @return Promise Object      Enumeration of differences
  */
	getResultSetDiff: function getResultSetDiff(client, currentData, query, params) {
		var oldHashes, result, diff, newData;
		return _regeneratorRuntime.async(function getResultSetDiff$(context$1$0) {
			while (1) switch (context$1$0.prev = context$1$0.next) {
				case 0:
					oldHashes = currentData.map(function (row) {
						return row._hash;
					});
					context$1$0.next = 3;
					return exports.performQuery(client, "\n\t\t\tWITH\n\t\t\t\tres AS (" + query + "),\n\t\t\t\tdata AS (\n\t\t\t\t\tSELECT\n\t\t\t\t\t\tMD5(CAST(ROW_TO_JSON(res.*) AS TEXT)) AS _hash,\n\t\t\t\t\t\tROW_NUMBER() OVER () AS _index,\n\t\t\t\t\t\tres.*\n\t\t\t\t\tFROM res),\n\t\t\t\tdata2 AS (\n\t\t\t\t\tSELECT\n\t\t\t\t\t\t1 AS _added,\n\t\t\t\t\t\tdata.*\n\t\t\t\t\tFROM data\n\t\t\t\t\tWHERE _hash NOT IN ('" + oldHashes.join("','") + "'))\n\t\t\tSELECT\n\t\t\t\tdata2.*,\n\t\t\t\tdata._hash AS _hash\n\t\t\tFROM data\n\t\t\tLEFT JOIN data2\n\t\t\t\tON (data._index = data2._index)", params);

				case 3:
					result = context$1$0.sent;
					diff = collectionDiff(oldHashes, result.rows);

					if (!(diff === null)) {
						context$1$0.next = 7;
						break;
					}

					return context$1$0.abrupt("return", null);

				case 7:
					newData = exports.applyDiff(currentData, diff);
					return context$1$0.abrupt("return", { diff: diff, data: newData });

				case 9:
				case "end":
					return context$1$0.stop();
			}
		}, null, this);
	},

	/**
  * Apply a diff to a result set
  * @param  Array  data Last known full result set
  * @param  Object diff Output from getResultSetDiff()
  * @return Array       New result set
  */
	applyDiff: function applyDiff(data, diff) {
		var newResults = data.slice();

		diff.removed !== null && diff.removed.forEach(function (removed) {
			return newResults[removed._index - 1] = undefined;
		});

		// Deallocate first to ensure no overwrites
		diff.moved !== null && diff.moved.forEach(function (moved) {
			newResults[moved.old_index - 1] = undefined;
		});

		diff.copied !== null && diff.copied.forEach(function (copied) {
			var copyRow = _.clone(data[copied.orig_index - 1]);
			copyRow._index = copied.new_index;
			newResults[copied.new_index - 1] = copyRow;
		});

		diff.moved !== null && diff.moved.forEach(function (moved) {
			var movingRow = data[moved.old_index - 1];
			movingRow._index = moved.new_index;
			newResults[moved.new_index - 1] = movingRow;
		});

		diff.added !== null && diff.added.forEach(function (added) {
			return newResults[added._index - 1] = added;
		});

		return newResults.filter(function (row) {
			return row !== undefined;
		});
	} };

// Helper for getDiffFromSupplied
function flattenNotifications(notifications) {
	var out = [];
	var pushItem = function (payload, key, index) {
		var data = _.clone(payload[key][0]);
		data._op = payload.op;
		data._key = key;
		data._index = index;
		out.push(data);
	};

	notifications.forEach(function (payload, index) {
		if (payload.op === "UPDATE") {
			pushItem(payload, "new_data", index);
			pushItem(payload, "old_data", index);
		} else {
			pushItem(payload, "data", index);
		}
	});

	return out;
}

// Force full refresh