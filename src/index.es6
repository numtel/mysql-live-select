var anyDB        = require('any-db');

var PgTriggers = require('./PgTriggers');

const CONN_STR = 'postgres://meteor:meteor@127.0.0.1/meteor';

// Initialize
var conn = anyDB.createConnection(CONN_STR);

// Create a trigger manager for this connection
// Each connection should run on its own unique channel (2nd arg)
var triggers = new PgTriggers(conn, 'test');


function liveClassScores(triggers, classId, onUpdate) {
  if(typeof triggers !== 'object' || typeof triggers.select !== 'function')
    throw new Error('first argument must be trigger manager object');
  if(typeof classId !== 'number' || !Number.isInteger(classId))
    throw new Error('classId must be integer');
  if(typeof onUpdate !== 'function')
    throw new Error('onUpdate callback must be defined');

  // Triggers require caches of active primary keys
  var assignmentIds = [], studentIds = [];

  // Prepare supporting query to main query
  var classAssignments = triggers.select(
    `SELECT id FROM assignments WHERE class_id = ${classId}`,
    { assignments: (class_id) => class_id === classId });

  classAssignments.on('update', function(results) {
    assignmentIds = results.map(row => row.id);
  });

  classAssignments.on('ready', function() {
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

    mySelect.on('update', function(results) {
      // Update student_id cache
      studentIds = results.map(row => row.student_id);

      onUpdate(results);
    });
  });
};

liveClassScores(triggers, 1, (results) => {
  console.log(results)
});


process.on('SIGINT', function() {
  // Ctrl+C
  triggers.cleanup((error, results) => {
    if(error) throw error;
    conn.end();
    process.exit();
  });
});



