"use strict";

var _core = require("babel-runtime/core-js")["default"];

/**
 * Execute a sequence of queries on a pg client in a transaction
 * @param  Object   client   The database client, or PgTriggers instance to
 *                            obtain a client automatically
 * @param  Boolean  debug    Print queries as they execute (optional)
 * @param  [String] queries  Queries to execute, in order
 * @param  Function callback Optional, call when complete (error, results)
 * @return Promise
 */
module.exports = function (client, debug, queries, callback) {
	if (debug instanceof Array) {
		callback = queries;
		queries = debug;
		debug = false;
	}

	return new _core.Promise(function (resolve, reject) {
		var results = [];

		if (typeof client.getClient === "function") {
			// PgTriggers instance passed as client, obtain client
			return client.getClient(function (error, client, done) {
				return module.exports(client, debug, queries, callback).then(function (results) {
					done();resolve(results);
				}, function (error) {
					done();reject(error);
				});
			});
		}

		if (queries.length === 0) {
			resolve();
			return callback && callback();
		}

		var sequence = queries.map(function (query, index, initQueries) {
			return function () {
				debug && console.log("QUERY", index, query);

				var queryComplete = function (error, rows, fields) {
					if (error) {
						client.query("ROLLBACK", function (rollbackError, result) {
							reject(rollbackError || error);
							return callback && callback(rollbackError || error);
						});
						return;
					}

					results.push(rows);

					if (index < sequence.length - 1) {
						sequence[index + 1]();
					} else {
						client.query("COMMIT", function (error, result) {
							if (error) {
								reject(error);
								return callback && callback(error);
							}
							resolve(results);
							return callback && callback(null, results);
						});
					}
				};

				if (query instanceof Array) {
					client.query(query[0], query[1], queryComplete);
				} else {
					client.query(query, queryComplete);
				}
			};
		});

		client.query("BEGIN", function (error, result) {
			if (error) {
				reject(error);
				return callback && callback(error);
			}
			sequence[0]();
		});
	});
};

/**
 * querySequence.noTx()
 * Perform a query sequence without a transaction
 */
module.exports.noTx = function (client, debug, queries, callback) {
	if (debug instanceof Array) {
		callback = queries;
		queries = debug;
		debug = false;
	}

	return new _core.Promise(function (resolve, reject) {
		var results = [];

		if (typeof client.getClient === "function") {
			// PgTriggers instance passed as client, obtain client
			return client.getClient(function (error, client, done) {
				return module.exports(client, debug, queries, callback).then(function (results) {
					done();resolve(results);
				}, function (error) {
					done();reject(error);
				});
			});
		}

		if (queries.length === 0) {
			resolve();
			return callback && callback();
		}

		var sequence = queries.map(function (query, index, initQueries) {
			var tmpCallback = function tmpCallback(error, rows, fields) {
				if (error) {
					reject(error);
					return callback(error);
				}

				results.push(rows);

				if (index < sequence.length - 1) {
					sequence[index + 1]();
				} else {
					resolve(results);
					return callback(null, results);
				}
			};

			return function () {
				debug && console.log("Query Sequence", index, query);

				if (query instanceof Array) {
					client.query(query[0], query[1], tmpCallback);
				} else {
					client.query(query, tmpCallback);
				}
			};
		});

		sequence[0]();
	});
};