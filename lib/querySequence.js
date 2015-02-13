"use strict";

// Execute a sequence of queries on a database connection
// @param {object} client - The database client
// @param {boolean} debug - Print queries as they execute (optional)
// @param {[string]} queries - Queries to execute, in order
// @param {function} callback - Call when complete (error, results)
module.exports = function (client, debug, queries, callback) {
  if (debug instanceof Array) {
    callback = queries;
    queries = debug;
    debug = false;
  }

  var results = [];

  client.query("BEGIN", function (error, result) {
    if (error) return callback(error);

    var sequence = queries.map(function (query, index, initQueries) {
      var tmpCallback = function (error, rows, fields) {
        if (error) {
          client.query("ROLLBACK", function (rollbackError, result) {
            callback(rollbackError || error);
          });
        }

        results.push(rows);

        if (index < sequence.length - 1) {
          sequence[index + 1]();
        } else {
          client.query("COMMIT", function (error, result) {
            if (error) return callback(error);
            return callback(null, results);
          });
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