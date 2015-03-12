var _            = require('lodash')
var randomString = require('random-strings')

var oldHashes = _.range(100).map(index => randomString.alphaLower(32))

module.exports = {
	init: {
		classCount: 20,
		assignPerClass: 10,
		studentsPerClass: 10,
		classesPerStudent: 6
	},
	customRunner: 'common.performQuery',
	clientCount: 50,
	query: `
		WITH
			res AS (
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
			),
			data AS (
				SELECT
					MD5(CAST(ROW_TO_JSON(res.*) AS TEXT)) AS _hash,
					ROW_NUMBER() OVER () AS _index,
					res.*
				FROM res),
			data2 AS (
				SELECT
					1 AS _added,
					data.*
				FROM data
				WHERE _hash NOT IN ('${oldHashes.join("','")}'))
		SELECT
			data2.*,
			data._hash AS _hash
		FROM data
		LEFT JOIN data2
			ON (data._index = data2._index)`,
	params: [ 1 ]
}

