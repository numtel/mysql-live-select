var util = require('util')
var LiveSQL = require('./LiveSQL')

const CONN_STR = 'postgres://meteor:meteor@127.0.0.1/meteor'
const CHANNEL = 'ben_test'

var liveDb = new LiveSQL(CONN_STR, CHANNEL)

var liveClassScores = function(liveDb, classId, onUpdate) {
	var assignmentIds = [], studentIds = []

	// Prepare supporting query
	var support = liveDb.select(
		`SELECT id FROM assignments WHERE class_id = $1`, [ classId ],
		(diff, results) => { assignmentIds = results.map(row => row.id) },
		{ assignments: row => row.class_id === classId }
	)

	var main = liveDb.select(`
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
	`, [ classId ], (diff, results) => {
		// Update student_id cache
		studentIds = results.map(row => row.student_id);
		onUpdate(diff, results)
	}, {
		assignments: row => row.class_id === classId,
		students: row => studentIds.indexOf(row.id) !== -1,
		scores: row => {
			return assignmentIds.indexOf(row.assignment_id) !== -1
		}
	})

	var stop = async function() {
		(await main).stop()
		(await support).stop()
		assignmentIds = null
		studentIds = null
	}

	return { stop }
}

var scoresHandle = liveClassScores(liveDb, 1, (diff, rows) => {
	console.log(util.inspect(diff, { depth: null }), rows)
})

// Ctrl+C
process.on('SIGINT', async function() {
	(await scoresHandle).stop()
	await liveDb.cleanup()
	process.exit()
})

