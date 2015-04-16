var EventEmitter = require('events').EventEmitter
var util = require('util')
var LivePG = require('./LivePG')

const CONN_STR = 'postgres://meteor:meteor@127.0.0.1/meteor'
const CHANNEL = 'ben_test'

var liveDb = new LivePG(CONN_STR, CHANNEL)

class liveClassScores extends EventEmitter {
  constructor(liveDb, classId) {
    var assignmentIds = [], studentIds = []

    // Prepare supporting query
    this.support = liveDb.select(
      `SELECT id FROM assignments WHERE class_id = $1`, [ classId ],
      { assignments: row => row.class_id === classId }
    )

    this.support.on('update', (diff, results) => {
      assignmentIds = results.map(row => row.id)
    })

    // Prepare main query
    this.main = liveDb.select(`
      SELECT
        students.name  AS student_name,
        students.id    AS student_id,
        assignments.id AS assignment_id,
        scores.id      AS score_id,
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
        assignments.class_id = $1
      ORDER BY
        score DESC
    `, [ classId ], {
      assignments: row => row.class_id === classId,
      students: row => studentIds.indexOf(row.id) !== -1,
      scores: row => {
        return assignmentIds.indexOf(row.assignment_id) !== -1
      }
    })

    this.main.on('update', (diff, results) => {
      // Update student_id cache
      studentIds = results.map(row => row.student_id);
      this.emit('update', diff, results)
    })
  }

  stop() {
    this.main.stop()
    this.support.stop()
  }
}

var scoresHandle = new liveClassScores(liveDb, 1)

scoresHandle.on('update', (diff, rows) => {
  console.log(util.inspect(diff, { depth: null }), rows)
})

// Ctrl+C
process.on('SIGINT', async function() {
  scoresHandle.stop()
  await liveDb.cleanup()
  process.exit()
})

