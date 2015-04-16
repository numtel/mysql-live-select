"use strict";

var util = require("util");
var LivePG = require("./LivePG");

var CONN_STR = "postgres://meteor:meteor@127.0.0.1/meteor";
var CHANNEL = "ben_test";

var liveDb = new LivePG(CONN_STR, CHANNEL);

liveDb.select("\n  SELECT\n    students.name  AS student_name,\n    students.id    AS student_id,\n    assignments.id AS assignment_id,\n    scores.id      AS score_id,\n    assignments.name,\n    assignments.value,\n    scores.score\n  FROM\n    scores\n  INNER JOIN assignments ON\n    (assignments.id = scores.assignment_id)\n  INNER JOIN students ON\n    (students.id = scores.student_id)\n  WHERE\n    assignments.class_id = $1\n  ORDER BY\n    score DESC\n", [1]).on("update", function (diff, rows) {
  console.log(util.inspect(diff, { depth: null }), rows);
});

// Ctrl+C
process.on("SIGINT", function () {
  liveDb.cleanup().then(process.exit);
});