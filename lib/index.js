"use strict";

var anyDB = require("any-db");

var PgTriggers = require("./PgTriggers");

var CONN_STR = "postgres://meteor:meteor@127.0.0.1/meteor";

// Initialize
var conn = anyDB.createConnection(CONN_STR);

// Create a trigger manager for this connection
// Each connection should run on its own unique channel (2nd arg)
var triggers = new PgTriggers(conn, "test");

// TODO: Perform joined query to figure out how to build efficient trigger
//        lambda, somehow testing if column value is in current result set?
var mySelect = triggers.select(
// Specify query string
"SELECT * FROM test_pub WHERE last_name = 'Palmer'",
// Specify trigger lambdas for each table to watch
// Arguments are grabbed from row changes and placed in NOTIFY payload
// Return boolean whether or not to refresh the query
{ test_pub: function (last_name) {
    return last_name === "Palmer";
  } });

mySelect.on("update", function (results) {
  console.log(results);
});


process.on("SIGINT", function () {
  // Ctrl+C
  triggers.cleanup(function (error, results) {
    if (error) throw error;
    conn.end();
    process.exit();
  });
});