var EventEmitter = require('events').EventEmitter;
var anyDB        = require('any-db');

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

    // Triggers require caches of active primary keys
    var assignmentIds = [], studentIds = [];

    // Prepare supporting query to main query
    var classAssignments = triggers.select(
      `SELECT id FROM assignments WHERE class_id = ${classId}`,
      { assignments: (class_id) => class_id === classId });

    classAssignments.on('update', (results) => {
      assignmentIds = results.map(row => row.id);
    });

    classAssignments.on('ready', () => {
      // Perform main query when supporting query is installed
      var mySelect = triggers.select(`
        SELECT
          students.name AS student_name,
          students.id AS student_id,
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
          assignments.class_id = ${classId}
      `, {
        assignments: (class_id) => class_id === classId,
        students: (id) => studentIds.indexOf(id) !== -1,
        scores: (assignment_id) => assignmentIds.indexOf(assignment_id) !== -1
      });

      mySelect.on('update', (results) => {
        // Update student_id cache
        studentIds = results.map(row => row.student_id);

        this.emit('update', results);
      });

      mySelect.on('diff', (diff) => {
        this.emit('diff', diff);
      });
    });
  }
}

var myClassScores = new liveClassScores(triggers, 1);

myClassScores.on('diff', (diff) => {
  console.log(diff)
});


process.on('SIGINT', function() {
  // Ctrl+C
  triggers.cleanup((error, results) => {
    if(error) throw error;
    conn.end();
    process.exit();
  });
});



