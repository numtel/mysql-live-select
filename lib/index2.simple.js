"use strict";

var util = require("util");
var LiveSQL = require("./LiveSQL");

var CONN_STR = "postgres://meteor:meteor@127.0.0.1/meteor";
var CHANNEL = "ben_test";

var liveDb = new LiveSQL(CONN_STR, CHANNEL);

liveDb.select("\n\tSELECT\n\t\t*\n\tFROM\n\t\tscores\n\tORDER BY\n\t\tscore DESC\n", function (diff, rows) {
	console.log(util.inspect(diff, { depth: null }), rows);
});

// Ctrl+C
process.on("SIGINT", function () {
	liveDb.cleanup().then(process.exit);
});