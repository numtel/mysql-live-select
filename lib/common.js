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
		var allRows, matched, newData, hasDelete, _iteratorNormalCompletion, _didIteratorError, _iteratorError, _iterator, _step, curIndex, curRow, _iteratorNormalCompletion2, _didIteratorError2, _iteratorError2, _iterator2, _step2, matchRow, isSame, _iteratorNormalCompletion3, _didIteratorError3, _iteratorError3, _iterator3, _step3, column, _iteratorNormalCompletion4, _didIteratorError4, _iteratorError4, _iterator4, _step4, addedRow, sortProps, sortOrders, _iteratorNormalCompletion5, _didIteratorError5, _iteratorError5, _iterator5, _step5, index, oldHashes, diff;

		return _regeneratorRuntime.async(function getDiffFromSupplied$(context$1$0) {
			while (1) switch (context$1$0.prev = context$1$0.next) {
				case 0:
					allRows = flattenNotifications(notifications);
					matched = matchRows(allRows, parsed, params);
					newData = currentData.slice();
					hasDelete = false;
					_iteratorNormalCompletion = true;
					_didIteratorError = false;
					_iteratorError = undefined;
					context$1$0.prev = 7;
					_iterator = _core.$for.getIterator(_.range(currentData.length));

				case 9:
					if (_iteratorNormalCompletion = (_step = _iterator.next()).done) {
						context$1$0.next = 68;
						break;
					}

					curIndex = _step.value;
					curRow = currentData[curIndex];
					_iteratorNormalCompletion2 = true;
					_didIteratorError2 = false;
					_iteratorError2 = undefined;
					context$1$0.prev = 15;
					_iterator2 = _core.$for.getIterator(matched);

				case 17:
					if (_iteratorNormalCompletion2 = (_step2 = _iterator2.next()).done) {
						context$1$0.next = 51;
						break;
					}

					matchRow = _step2.value;
					isSame = true;
					_iteratorNormalCompletion3 = true;
					_didIteratorError3 = false;
					_iteratorError3 = undefined;
					context$1$0.prev = 23;
					_iterator3 = _core.$for.getIterator(_core.Object.keys(curRow));

				case 25:
					if (_iteratorNormalCompletion3 = (_step3 = _iterator3.next()).done) {
						context$1$0.next = 33;
						break;
					}

					column = _step3.value;

					if (!(column !== "_hash" && column !== "_index" && curRow[column] !== matchRow[column])) {
						context$1$0.next = 30;
						break;
					}

					isSame = false;
					return context$1$0.abrupt("break", 33);

				case 30:
					_iteratorNormalCompletion3 = true;
					context$1$0.next = 25;
					break;

				case 33:
					context$1$0.next = 39;
					break;

				case 35:
					context$1$0.prev = 35;
					context$1$0.t4 = context$1$0["catch"](23);
					_didIteratorError3 = true;
					_iteratorError3 = context$1$0.t4;

				case 39:
					context$1$0.prev = 39;
					context$1$0.prev = 40;

					if (!_iteratorNormalCompletion3 && _iterator3["return"]) {
						_iterator3["return"]();
					}

				case 42:
					context$1$0.prev = 42;

					if (!_didIteratorError3) {
						context$1$0.next = 45;
						break;
					}

					throw _iteratorError3;

				case 45:
					return context$1$0.finish(42);

				case 46:
					return context$1$0.finish(39);

				case 47:

					if (isSame === true && (matchRow._op === "DELETE" || matchRow._op === "UPDATE" && matchRow._key === "old_data")) {

						newData[curIndex] = undefined;
						hasDelete = true;
					}

				case 48:
					_iteratorNormalCompletion2 = true;
					context$1$0.next = 17;
					break;

				case 51:
					context$1$0.next = 57;
					break;

				case 53:
					context$1$0.prev = 53;
					context$1$0.t5 = context$1$0["catch"](15);
					_didIteratorError2 = true;
					_iteratorError2 = context$1$0.t5;

				case 57:
					context$1$0.prev = 57;
					context$1$0.prev = 58;

					if (!_iteratorNormalCompletion2 && _iterator2["return"]) {
						_iterator2["return"]();
					}

				case 60:
					context$1$0.prev = 60;

					if (!_didIteratorError2) {
						context$1$0.next = 63;
						break;
					}

					throw _iteratorError2;

				case 63:
					return context$1$0.finish(60);

				case 64:
					return context$1$0.finish(57);

				case 65:
					_iteratorNormalCompletion = true;
					context$1$0.next = 9;
					break;

				case 68:
					context$1$0.next = 74;
					break;

				case 70:
					context$1$0.prev = 70;
					context$1$0.t6 = context$1$0["catch"](7);
					_didIteratorError = true;
					_iteratorError = context$1$0.t6;

				case 74:
					context$1$0.prev = 74;
					context$1$0.prev = 75;

					if (!_iteratorNormalCompletion && _iterator["return"]) {
						_iterator["return"]();
					}

				case 77:
					context$1$0.prev = 77;

					if (!_didIteratorError) {
						context$1$0.next = 80;
						break;
					}

					throw _iteratorError;

				case 80:
					return context$1$0.finish(77);

				case 81:
					return context$1$0.finish(74);

				case 82:
					if (!(hasDelete === true && parsed.limit && parsed.limit.value.value === currentData.length)) {
						context$1$0.next = 86;
						break;
					}

					context$1$0.next = 85;
					return exports.getResultSetDiff(client, currentData, query, params);

				case 85:
					return context$1$0.abrupt("return", context$1$0.sent);

				case 86:
					_iteratorNormalCompletion4 = true;
					_didIteratorError4 = false;
					_iteratorError4 = undefined;
					context$1$0.prev = 89;

					for (_iterator4 = _core.$for.getIterator(matched); !(_iteratorNormalCompletion4 = (_step4 = _iterator4.next()).done); _iteratorNormalCompletion4 = true) {
						matchRow = _step4.value;

						if (matchRow._op === "INSERT" || matchRow._op === "UPDATE" && matchRow._key === "new_data") {
							addedRow = _.clone(matchRow);

							// All extra fields must be removed for hashing
							delete addedRow._op;
							delete addedRow._key;
							delete addedRow._index;

							addedRow._hash = md5.digest_s(JSON.stringify(addedRow));
							addedRow._added = 1;
							newData.push(addedRow);
						}
					}

					context$1$0.next = 97;
					break;

				case 93:
					context$1$0.prev = 93;
					context$1$0.t7 = context$1$0["catch"](89);
					_didIteratorError4 = true;
					_iteratorError4 = context$1$0.t7;

				case 97:
					context$1$0.prev = 97;
					context$1$0.prev = 98;

					if (!_iteratorNormalCompletion4 && _iterator4["return"]) {
						_iterator4["return"]();
					}

				case 100:
					context$1$0.prev = 100;

					if (!_didIteratorError4) {
						context$1$0.next = 103;
						break;
					}

					throw _iteratorError4;

				case 103:
					return context$1$0.finish(100);

				case 104:
					return context$1$0.finish(97);

				case 105:
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

					_iteratorNormalCompletion5 = true;
					_didIteratorError5 = false;
					_iteratorError5 = undefined;
					context$1$0.prev = 111;
					// Fix indexes
					for (_iterator5 = _core.$for.getIterator(_.range(newData.length)); !(_iteratorNormalCompletion5 = (_step5 = _iterator5.next()).done); _iteratorNormalCompletion5 = true) {
						index = _step5.value;

						newData[index]._index = index + 1;
					}

					context$1$0.next = 119;
					break;

				case 115:
					context$1$0.prev = 115;
					context$1$0.t8 = context$1$0["catch"](111);
					_didIteratorError5 = true;
					_iteratorError5 = context$1$0.t8;

				case 119:
					context$1$0.prev = 119;
					context$1$0.prev = 120;

					if (!_iteratorNormalCompletion5 && _iterator5["return"]) {
						_iterator5["return"]();
					}

				case 122:
					context$1$0.prev = 122;

					if (!_didIteratorError5) {
						context$1$0.next = 125;
						break;
					}

					throw _iteratorError5;

				case 125:
					return context$1$0.finish(122);

				case 126:
					return context$1$0.finish(119);

				case 127:
					oldHashes = currentData.map(function (row) {
						return row._hash;
					});
					diff = collectionDiff(oldHashes, newData);

					if (!(diff === null)) {
						context$1$0.next = 131;
						break;
					}

					return context$1$0.abrupt("return", null);

				case 131:
					return context$1$0.abrupt("return", { diff: diff, data: newData });

				case 132:
				case "end":
					return context$1$0.stop();
			}
		}, null, this, [[7, 70, 74, 82], [15, 53, 57, 65], [23, 35, 39, 47], [40,, 42, 46], [58,, 60, 64], [75,, 77, 81], [89, 93, 97, 105], [98,, 100, 104], [111, 115, 119, 127], [120,, 122, 126]]);
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