var EventEmitter = require('events').EventEmitter
var _ = require('lodash')
var LivePG = require('../../../')

var liveDb = global.liveDb = new LivePG(options.conn, options.channel)

liveDb.on('error', function(error) {
	console.error(error)
})

var selectCount = 
	settings.maxSelects && settings.maxSelects < settings.init.classCount ?
		settings.maxSelects : settings.init.classCount

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

module.exports = _.flatten(_.range(settings.instanceMultiplier || 1)
	.map(instance => _.range(selectCount).map(index => {

	var select = new liveClassScores(liveDb, index + 1)

	select.on('update', (diff, rows) => {
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

