"use strict";

var _ = require("lodash");
var pg = require("pg");

var PgTriggers = require("./PgTriggers");

var CONN_STR = "postgres://meteor:meteor@127.0.0.1/meteor";

var triggers, clientDone;

pg.connect(CONN_STR, function (error, client, done) {
  triggers = new PgTriggers(client, "ben_test_channel");
  clientDone = done;

  var mySelect = triggers.select("\n    SELECT\n      students.name  AS student_name,\n      students.id    AS student_id,\n      assignments.id AS assignment_id,\n      scores.id      AS score_id,\n      assignments.name,\n      assignments.value,\n      scores.score\n    FROM\n      scores\n    INNER JOIN assignments ON\n      (assignments.id = scores.assignment_id)\n    INNER JOIN students ON\n      (students.id = scores.student_id)\n    WHERE\n      assignments.class_id = $1\n  ", [1]);

  mySelect.on("update", function (diff) {
    console.log(diff);
  });
});

process.on("SIGINT", function () {
  // Ctrl+C
  triggers.cleanup(function (error, results) {
    if (error) throw error;
    clientDone();
    process.exit();
  });
});