"use strict";

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var EventEmitter = require("events").EventEmitter;
var anyDB = require("any-db");
var _ = require("lodash");

var PgTriggers = require("./PgTriggers");
var querySequence = require("./querySequence");

var CONN_STR = "postgres://meteor:meteor@127.0.0.1/meteor";

// Initialize
var conn = anyDB.createConnection(CONN_STR);

conn.setMaxListeners(0);

var liveClassScores = (function (EventEmitter) {
  function liveClassScores(triggers, classId, score) {
    var _this = this;
    _classCallCheck(this, liveClassScores);

    if (typeof triggers !== "object" || typeof triggers.select !== "function") throw new Error("first argument must be trigger manager object");
    if (typeof classId !== "number") throw new Error("classId must be integer");

    var mySelect = triggers.select("\n      SELECT\n        students.name  AS student_name,\n        students.id    AS student_id,\n        assignments.id AS assignment_id,\n        assignments.name,\n        assignments.value,\n        scores.score\n      FROM\n        scores\n      INNER JOIN assignments ON\n        (assignments.id = scores.assignment_id)\n      INNER JOIN students ON\n        (students.id = scores.student_id)\n      WHERE\n        assignments.class_id = $1 AND scores.score BETWEEN $2 AND ($2 + 5)\n    ", [classId, score]);

    mySelect.on("update", function (results, allRows) {
      _this.emit("update", results, allRows);
    });

    mySelect.on("ready", function (results) {
      _this._ready = true;
      _this.emit("ready", results);
    });
  }

  _inherits(liveClassScores, EventEmitter);

  return liveClassScores;
})(EventEmitter);

var triggers = [];
var scores = [];
var startDate = new Date();
var endDate = null;

function end() {
  endDate = new Date();

  console.log("finished in", endDate - startDate - 10000);
}

var throttledEnd = _.throttle(end, 10000, { leading: false });

conn.query("TRUNCATE scores", function (error, result) {
  // Create a trigger manager for this connection
  // Each connection should run on its own unique channel (2nd arg)
  for (var i = 0; i < 10; i++) {
    triggers[i] = new PgTriggers(conn, "test" + i);
    scores[i] = new liveClassScores(triggers[i], 1, i * 10);

    scores[i].on("update", (function (i, results, allRows) {
      console.log(i, results);

      if (!endDate) {
        throttledEnd();
      }
    }).bind(undefined, i));

    scores[i].on("ready", (function (i) {
      var ready = !scores.filter(function (tmpScores) {
        return !tmpScores._ready;
      }).length;

      if (ready) {
        test();
      }
    }).bind(undefined, i));
  }
});

function test() {
  // Get some values to use
  var init = ["SELECT id FROM students", "SELECT id FROM assignments"];

  var studentIds = [];
  var assignmentIds = [];

  querySequence(conn, init, function (error, result) {
    studentIds = result[0].rows.map(function (row) {
      return row.id;
    });
    assignmentIds = result[1].rows.map(function (row) {
      return row.id;
    });

    var sql = [];
    var rows = [];

    for (var i = 0; i < 500; i++) {
      var studentId = choice(studentIds);
      var assignmentId = choice(assignmentIds);
      var score = Math.random() * 100;

      rows.push("\n          (" + studentId + ", " + assignmentId + ", " + score + ")\n      ");
    }

    sql.push("\n      INSERT INTO scores\n        (student_id, assignment_id, score)\n      VALUES\n        " + rows.join(", ") + "\n    ");

    querySequence(conn, sql, function (error, result) {
      if (error) return console.log(error);
    });
  });
}

function choice(items) {
  var i = Math.floor(Math.random() * items.length);
  return items[i];
}

process.on("SIGINT", function () {
  // Ctrl+C
  for (var i in triggers) {
    triggers[i].cleanup((function (i, error, results) {
      if (error) throw error;

      triggers[i]._done = true;

      if (!triggers.filter(function (trigger) {
        return !trigger._done;
      }).length) {
        conn.end();
        process.exit();
      }
    }).bind(this, i));
  }
});