"use strict";

var _classCallCheck = require("babel-runtime/helpers/class-call-check")["default"];

var _inherits = require("babel-runtime/helpers/inherits")["default"];

var _createClass = require("babel-runtime/helpers/create-class")["default"];

var _regeneratorRuntime = require("babel-runtime/regenerator")["default"];

var EventEmitter = require("events").EventEmitter;
var util = require("util");
var LivePG = require("./LivePG");

var CONN_STR = "postgres://meteor:meteor@127.0.0.1/meteor";
var CHANNEL = "ben_test";

var liveDb = new LivePG(CONN_STR, CHANNEL);

var liveClassScores = (function (_EventEmitter) {
	function liveClassScores(liveDb, classId) {
		var _this = this;

		_classCallCheck(this, liveClassScores);

		var assignmentIds = [],
		    studentIds = [];

		// Prepare supporting query
		this.support = liveDb.select("SELECT id FROM assignments WHERE class_id = $1", [classId], { assignments: function (row) {
				return row.class_id === classId;
			} });

		this.support.on("update", function (diff, results) {
			assignmentIds = results.map(function (row) {
				return row.id;
			});
		});

		// Prepare main query
		this.main = liveDb.select("\n\t\t\tSELECT\n\t\t\t\tstudents.name  AS student_name,\n\t\t\t\tstudents.id    AS student_id,\n\t\t\t\tassignments.id AS assignment_id,\n\t\t\t\tscores.id      AS score_id,\n\t\t\t\tassignments.name,\n\t\t\t\tassignments.value,\n\t\t\t\tscores.score\n\t\t\tFROM\n\t\t\t\tscores\n\t\t\tINNER JOIN assignments ON\n\t\t\t\t(assignments.id = scores.assignment_id)\n\t\t\tINNER JOIN students ON\n\t\t\t\t(students.id = scores.student_id)\n\t\t\tWHERE\n\t\t\t\tassignments.class_id = $1\n\t\t\tORDER BY\n\t\t\t\tscore DESC\n\t\t", [classId], {
			assignments: function (row) {
				return row.class_id === classId;
			},
			students: function (row) {
				return studentIds.indexOf(row.id) !== -1;
			},
			scores: function (row) {
				return assignmentIds.indexOf(row.assignment_id) !== -1;
			}
		});

		this.main.on("update", function (diff, results) {
			// Update student_id cache
			studentIds = results.map(function (row) {
				return row.student_id;
			});
			_this.emit("update", diff, results);
		});
	}

	_inherits(liveClassScores, _EventEmitter);

	_createClass(liveClassScores, {
		stop: {
			value: function stop() {
				this.main.stop();
				this.support.stop();
			}
		}
	});

	return liveClassScores;
})(EventEmitter);

var scoresHandle = new liveClassScores(liveDb, 1);

scoresHandle.on("update", function (diff, rows) {
	console.log(util.inspect(diff, { depth: null }), rows);
});

// Ctrl+C
process.on("SIGINT", function callee$0$0() {
	return _regeneratorRuntime.async(function callee$0$0$(context$1$0) {
		while (1) switch (context$1$0.prev = context$1$0.next) {
			case 0:
				scoresHandle.stop();
				context$1$0.next = 3;
				return liveDb.cleanup();

			case 3:
				process.exit();

			case 4:
			case "end":
				return context$1$0.stop();
		}
	}, null, this);
});