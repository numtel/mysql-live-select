var util = require('util')
var LivePG = require('./LivePG')

const CONN_STR = 'postgres://meteor:meteor@127.0.0.1/meteor'
const CHANNEL = 'ben_test'

var liveDb = new LivePG(CONN_STR, CHANNEL)

liveDb.select(`
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
`, [ 1 ]).on('update', (diff, rows) => {
	console.log(util.inspect(diff, { depth: null }), rows)
})

// Ctrl+C
process.on('SIGINT', () => {
	liveDb.cleanup().then(process.exit)
})

