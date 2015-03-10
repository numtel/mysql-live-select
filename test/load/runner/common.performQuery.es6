var _ = require('lodash')

var common = require('../../../src/common')

// Milliseconds to wait between closing one connection and opening another
const SLEEP_DURATION = 10


async function simpleQuery() {
	var conn = await common.getClient(options.conn)

	var result = await common.performQuery(conn.client, `
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
			score_id ASC
	`, [ 1 ])

	conn.done()
}

function simpleQueryForever() {
	simpleQuery().then(() => {
		setTimeout(() => {
			process.stdout.write(['NEXT_EVENT', Date.now()].join(' '))
			simpleQueryForever()
		}, SLEEP_DURATION)
	}, reason => console.error('performQuery Failed', reason))
}

_.range(settings.clientCount).map(simpleQueryForever)
