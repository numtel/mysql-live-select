var EventEmitter = require('events').EventEmitter;
var anyDB        = require('any-db');
var _            = require('lodash');

var PgTriggers = require('./PgTriggers');
var querySequence = require('./querySequence');

const CONN_STR = 'postgres://meteor:meteor@127.0.0.1/meteor';

// Initialize
var conn = anyDB.createConnection(CONN_STR);

conn.setMaxListeners(0);

class liveClassScores extends EventEmitter {
  constructor(triggers, classId, score) {
    if(typeof triggers !== 'object' || typeof triggers.select !== 'function')
      throw new Error('first argument must be trigger manager object');
    if(typeof classId !== 'number')
      throw new Error('classId must be integer');

    var mySelect = triggers.select(`
      SELECT
        students.name  AS student_name,
        students.id    AS student_id,
        assignments.id AS assignment_id,
        assignments.name,
        assignments.value,
        scores.score
      FROM
        scores
      INNER JOIN assignments ON
        (assignments.id = scores.assignment_id)
      INNER JOIN students ON
        (students.id = scores.student_id)
      WHERE
        assignments.class_id = $1 AND scores.score BETWEEN $2 AND $2 + 10
    `, [classId, score]);

    console.log(`
      SELECT
        students.name  AS student_name,
        students.id    AS student_id,
        assignments.id AS assignment_id,
        assignments.name,
        assignments.value,
        scores.score
      FROM
        scores
      INNER JOIN assignments ON
        (assignments.id = scores.assignment_id)
      INNER JOIN students ON
        (students.id = scores.student_id)
      WHERE
        assignments.class_id = ${classId} AND scores.score BETWEEN ${score} AND ${score} + 5
    `);

    mySelect.on('update', (results, allRows) => {
      this.emit('update', results, allRows);
    });

    mySelect.on('ready', (results) => {
      this._ready = true;
      this.emit('ready', results);
    });
  }
}

var triggers  = [];
var scores    = [];
var startDate = new Date();
var endDate   = null;

function end() {
  endDate = new Date();

  console.log('finished in', endDate - startDate - 1000);
}

var throttledEnd = _.throttle(end, 1000, { leading : false });

conn.query(`TRUNCATE scores`, (error, result) => {
  // Create a trigger manager for this connection
  // Each connection should run on its own unique channel (2nd arg)
  for(var i = 0; i < 10; i++) {
    triggers[i] = new PgTriggers(conn, `test${i}`);
    scores[i]   = new liveClassScores(triggers[i], 1, i * 10);

    scores[i].on('update', function(i, results, allRows) {
      for(var r in results) {
        console.log(`${i * 10} - ${i * 10 + 5}`, results[r][0], results[r][1]._id, results[r][1].score);
      }

      if(!endDate) {
        throttledEnd();
      }
    }.bind(this, i));

    scores[i].on('ready', (i) => {
      var ready = !scores.filter((tmpScores) => !tmpScores._ready).length;

      if(ready) {
        test();
      }
    }.bind(this, i));
  }
});

function test() {
  // Get some values to use
  var init = [
    `SELECT id FROM students`,
    `SELECT id FROM assignments`
  ];

  var studentIds    = [];
  var assignmentIds = [];

  querySequence(conn, init, (error, result) => {
    studentIds    = result[0].rows.map((row) => row.id);
    assignmentIds = result[1].rows.map((row) => row.id);

    var sql  = [];
    var rows = [];

    for(var i = 0; i < 100; i++) {
      var studentId    = choice(studentIds);
      var assignmentId = choice(assignmentIds);
      var score        = Math.ceil(Math.random() * 100);

      rows.push(`
          (${studentId}, ${assignmentId}, ${score})
      `);
    }

    sql.push(`
      INSERT INTO scores
        (student_id, assignment_id, score)
      VALUES
        ${rows.join(", ")}
    `);

    querySequence(conn, sql, (error, result) => {
      console.log('done inserting');
      if(error) return console.log(error);
    });
  });
}

function choice(items) {
  var i = Math.floor(Math.random() * items.length);
  return items[i];
}

process.on('SIGINT', function() {
  // Ctrl+C
  for(var i in triggers) {
    triggers[i].cleanup((i, error, results) => {
      if(error) throw error;

      triggers[i]._done = true;

      if(!triggers.filter((trigger) => !trigger._done).length) {
        conn.end();
        process.exit();
      }
    }.bind(this, i));
  }
});
