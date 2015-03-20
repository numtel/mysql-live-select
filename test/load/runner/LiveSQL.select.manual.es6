var _ = require('lodash')
var LiveSQL = require('../../../')

var liveDb = global.liveDb = new LiveSQL(options.conn, options.channel)

liveDb.on('error', function(error) {
	console.error(error)
})

var selectCount = 
	settings.maxSelects && settings.maxSelects < settings.init.classCount ?
		settings.maxSelects : settings.init.classCount

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
		studentIds = results.map(row => row.student_id)
		onUpdate(diff, results)
	}, {
		assignments: row => row.class_id === classId,
		students: row => studentIds.indexOf(row.id) !== -1,
		scores: row => assignmentIds.indexOf(row.assignment_id) !== -1
	})

	var stop = async function() {
		(await main).stop()
		(await support).stop()
		assignmentIds = null
		studentIds = null
	}

	return { stop }
}

module.exports = _.flatten(_.range(settings.instanceMultiplier || 1)
	.map(instance => _.range(selectCount).map(index => {

	var select = liveClassScores(liveDb, index + 1, (diff, rows) => {
		var scoreIds = ''
		if(diff.added) {
			scoreIds = diff.added.map(row => row.score_id + '@' + row.score).join(',')
		}
		process.stdout.write([
			'CLASS_UPDATE',
			Date.now(),
			index + 1,
			scoreIds
		].join(' '))
	})

	return select
})))

