"use strict";

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var EventEmitter = require("events").EventEmitter;
var anyDB = require("any-db");

var PgTriggers = require("./PgTriggers");

var CONN_STR = "postgres://meteor:meteor@127.0.0.1/meteor";

// Initialize
var conn = anyDB.createConnection(CONN_STR);

// Create a trigger manager for this connection
// Each connection should run on its own unique channel (2nd arg)
var triggers = new PgTriggers(conn, "test");

var liveClassScores = (function (EventEmitter) {
  function liveClassScores(triggers, classId) {
    var _this = this;
    _classCallCheck(this, liveClassScores);

    if (typeof triggers !== "object" || typeof triggers.select !== "function") throw new Error("first argument must be trigger manager object");
    if (typeof classId !== "number") throw new Error("classId must be integer");

    // Triggers require caches of active primary keys
    var assignmentIds = [],
        studentIds = [];

    // Prepare supporting query to main query
    var classAssignments = triggers.select("SELECT id FROM assignments WHERE class_id = " + classId, { assignments: function (class_id) {
        return class_id === classId;
      } });

    classAssignments.on("update", function (results) {
      assignmentIds = results.map(function (row) {
        return row.id;
      });
    });

    classAssignments.on("ready", function () {
      // Perform main query when supporting query is installed
      var mySelect = triggers.select("\n        SELECT\n          students.name AS student_name,\n          students.id AS student_id,\n          assignments.name,\n          assignments.value,\n          scores.score\n        FROM\n          scores\n        INNER JOIN assignments ON\n          (assignments.id = scores.assignment_id)\n        INNER JOIN students ON\n          (students.id = scores.student_id)\n        WHERE\n          assignments.class_id = " + classId + "\n      ", {
        assignments: function (class_id) {
          return class_id === classId;
        },
        students: function (id) {
          return studentIds.indexOf(id) !== -1;
        },
        scores: function (assignment_id) {
          return assignmentIds.indexOf(assignment_id) !== -1;
        }
      });

      mySelect.on("update", function (results) {
        // Update student_id cache
        studentIds = results.map(function (row) {
          return row.student_id;
        });

        _this.emit("update", results);
      });

      mySelect.on("diff", function (diff) {
        _this.emit("diff", diff);
      });
    });
  }

  _inherits(liveClassScores, EventEmitter);

  return liveClassScores;
})(EventEmitter);

var myClassScores = new liveClassScores(triggers, 1);

myClassScores.on("diff", function (diff) {
  console.log(diff);
});


process.on("SIGINT", function () {
  // Ctrl+C
  triggers.cleanup(function (error, results) {
    if (error) throw error;
    conn.end();
    process.exit();
  });
});