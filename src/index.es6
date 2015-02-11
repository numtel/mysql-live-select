var EventEmitter = require('events').EventEmitter;
var anyDB        = require('any-db');
var _            = require('lodash');

var PgTriggers = require('./PgTriggers');

const CONN_STR = 'postgres://meteor:meteor@127.0.0.1/meteor';

// Initialize
var conn = anyDB.createConnection(CONN_STR);

// Create a trigger manager for this connection
// Each connection should run on its own unique channel (2nd arg)
var triggers = new PgTriggers(conn, 'test');

class liveClassScores extends EventEmitter {
  constructor(triggers, classId) {
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
        assignments.class_id = ${classId} AND scores.score > 10
    `);

    mySelect.on('update', (results, allRows) => {
      this.emit('update', results, allRows);
    });
  }
}

var myClassScores = new liveClassScores(triggers, 1);

myClassScores.on('update', (diff, allRows) => {
  console.log(diff, _.keys(allRows).length);
});

process.on('SIGINT', function() {
  // Ctrl+C
  triggers.cleanup((error, results) => {
    if(error) throw error;
    conn.end();
    process.exit();
  });
});
