"use strict";

var _core = require("babel-runtime/core-js")["default"];

var _regeneratorRuntime = require("babel-runtime/regenerator")["default"];

var _ = require("lodash");
var pg = require("pg");
var randomString = require("random-strings");

var collectionDiff = require("./collectionDiff");

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

  delay: function delay() {
    var duration = arguments[0] === undefined ? 0 : arguments[0];

    return new _core.Promise(function (resolve, reject) {
      return setTimeout(resolve, duration);
    });
  },

  /**
   * Query information_schema to determine tables used
   * @param  Object client node-postgres client
   * @param  String query  SQL statement, params not used
   * @return Promise Array Table names
   * TODO change to EXPLAIN?
   */
  getQueryDetails: function getQueryDetails(client, query) {
    var nullifiedQuery, viewName, tablesResult;
    return _regeneratorRuntime.async(function getQueryDetails$(context$1$0) {
      while (1) switch (context$1$0.prev = context$1$0.next) {
        case 0:
          nullifiedQuery = query.replace(/\$\d+/g, "NULL");
          viewName = "tmp_view_" + randomString.alphaLower(10);
          context$1$0.next = 4;
          return exports.performQuery(client, "CREATE OR REPLACE TEMP VIEW " + viewName + " AS (" + nullifiedQuery + ")");

        case 4:
          context$1$0.next = 6;
          return exports.performQuery(client, "SELECT DISTINCT vc.table_name\n        FROM information_schema.view_column_usage vc\n        WHERE view_name = $1", [viewName]);

        case 6:
          tablesResult = context$1$0.sent;
          context$1$0.next = 9;
          return exports.performQuery(client, "DROP VIEW " + viewName);

        case 9:
          return context$1$0.abrupt("return", tablesResult.rows.map(function (row) {
            return row.table_name;
          }));

        case 10:
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
   * TODO notification pagination at 8000 bytes
   */
  createTableTrigger: function createTableTrigger(client, table, channel) {
    var triggerName, payloadTpl, payloadNew, payloadOld, payloadChanged;
    return _regeneratorRuntime.async(function createTableTrigger$(context$1$0) {
      while (1) switch (context$1$0.prev = context$1$0.next) {
        case 0:
          triggerName = "" + channel + "_" + table;
          payloadTpl = "\n      SELECT\n        '" + table + "'  AS table,\n        TG_OP       AS op,\n        json_agg($ROW$) AS data\n      INTO row_data;\n    ";
          payloadNew = payloadTpl.replace(/\$ROW\$/g, "NEW");
          payloadOld = payloadTpl.replace(/\$ROW\$/g, "OLD");
          payloadChanged = "\n      SELECT\n        '" + table + "'  AS table,\n        TG_OP       AS op,\n        json_agg(NEW) AS new_data,\n        json_agg(OLD) AS old_data\n      INTO row_data;\n    ";
          context$1$0.next = 7;
          return exports.performQuery(client, "CREATE OR REPLACE FUNCTION " + triggerName + "() RETURNS trigger AS $$\n        DECLARE\n          row_data   RECORD;\n          full_msg   TEXT;\n          full_len   INT;\n          cur_page   INT;\n          page_count INT;\n          msg_hash   TEXT;\n        BEGIN\n          IF (TG_OP = 'INSERT') THEN\n            " + payloadNew + "\n          ELSIF (TG_OP  = 'DELETE') THEN\n            " + payloadOld + "\n          ELSIF (TG_OP = 'UPDATE') THEN\n            " + payloadChanged + "\n          END IF;\n\n          SELECT row_to_json(row_data)::TEXT INTO full_msg;\n          SELECT char_length(full_msg)       INTO full_len;\n          SELECT (full_len / 7950) + 1       INTO page_count;\n          SELECT md5(full_msg)               INTO msg_hash;\n\n          FOR cur_page IN 1..page_count LOOP\n            PERFORM pg_notify('" + channel + "',\n              msg_hash || ':' || page_count || ':' || cur_page || ':' ||\n              substr(full_msg, ((cur_page - 1) * 7950) + 1, 7950)\n            );\n          END LOOP;\n          RETURN NULL;\n        END;\n      $$ LANGUAGE plpgsql");

        case 7:
          context$1$0.next = 9;
          return exports.performQuery(client, "DROP TRIGGER IF EXISTS \"" + triggerName + "\"\n        ON \"" + table + "\"");

        case 9:
          context$1$0.next = 11;
          return exports.performQuery(client, "CREATE TRIGGER \"" + triggerName + "\"\n        AFTER INSERT OR UPDATE OR DELETE ON \"" + table + "\"\n        FOR EACH ROW EXECUTE PROCEDURE " + triggerName + "()");

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
          return exports.performQuery(client, "\n      WITH\n        res AS (" + query + "),\n        data AS (\n          SELECT\n            res.*,\n            MD5(CAST(ROW_TO_JSON(res.*) AS TEXT)) AS _hash,\n            ROW_NUMBER() OVER () AS _index\n          FROM res),\n        data2 AS (\n          SELECT\n            1 AS _added,\n            data.*\n          FROM data\n          WHERE _hash NOT IN ('" + oldHashes.join("','") + "'))\n      SELECT\n        data2.*,\n        data._hash AS _hash\n      FROM data\n      LEFT JOIN data2\n        ON (data._index = data2._index)", params);

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