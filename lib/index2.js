"use strict";

var _ = require("lodash");
var pg = require("pg");

var PgTriggers = require("./PgTriggers");

var CONN_STR = "postgres://meteor:meteor@127.0.0.1/meteor";

var triggers, clientDone;

pg.connect(CONN_STR, function (error, client, done) {
	triggers = new PgTriggers(client, "ben_test_channel");
	clientDone = done;

	var mySelect = triggers.select("\n\t\tSELECT\n\t\t\tstudents.name  AS student_name,\n\t\t\tstudents.id    AS student_id,\n\t\t\tassignments.id AS assignment_id,\n\t\t\tscores.id      AS score_id,\n\t\t\tassignments.name,\n\t\t\tassignments.value,\n\t\t\tscores.score\n\t\tFROM\n\t\t\tscores\n\t\tINNER JOIN assignments ON\n\t\t\t(assignments.id = scores.assignment_id)\n\t\tINNER JOIN students ON\n\t\t\t(students.id = scores.student_id)\n\t\tWHERE\n\t\t\tassignments.class_id = $1\n\t", [1]);

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