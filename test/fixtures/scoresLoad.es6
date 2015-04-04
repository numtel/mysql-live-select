var _ = require('lodash')

var randomString  = require('random-strings')
var querySequence = require('../helpers/querySequence')

/**
 * Generate data structure describing a random scores set
 * @param Integer classCount        total number of classes to generate
 * @param Integer assignPerClass    number of assignments per class
 * @param Integer studentsPerClass  number of students enrolled in each class
 * @param Integer classesPerStudent number of classes each student is enrolled
 */
exports.generate =
function(classCount, assignPerClass, studentsPerClass, classesPerStudent) {
	var studentCount = Math.ceil(classCount / classesPerStudent) * studentsPerClass
	var assignCount  = classCount * assignPerClass
	var scoreCount   = assignCount * studentsPerClass

	var students = _.range(studentCount).map(index => {
		return {
			id   : index + 1,
			name : randomString.alphaLower(10)
		}
	})

	var assignments = _.range(assignCount).map(index => {
		return {
			id       : index + 1,
			class_id : (index % classCount) + 1,
			name     : randomString.alphaLower(10),
			value    : Math.ceil(Math.random() * 100)
		}
	})

	var scores = _.range(scoreCount).map(index => {
		var assignId = Math.floor(index / studentsPerClass) + 1
		var baseStudent =
			Math.floor((assignments[assignId - 1].class_id - 1) / classesPerStudent)

		return {
			id            : index + 1,
			assignment_id : assignId,
			student_id    : (baseStudent * studentsPerClass) +
			                (index % studentsPerClass) + 1,
			score         : Math.ceil(Math.random() * assignments[assignId - 1].value)
		}
	})

	return { assignments, students, scores }
}

function columnTypeFromName(name) {
	switch(name){
		case 'id'   : return 'serial NOT NULL'
		case 'name' : return 'character varying(50) NOT NULL'
		default     : return 'integer NOT NULL'
	}
}

/**
 * Create/replace test tables filled with fixture data
 * @param  Object   generatation     Output from generate() function above
 * @return Promise
 */
exports.install = function(generation) {
	return Promise.all(_.map(generation, (rows, table) => {

		// Create tables, Insert data
		var installQueries = [
			`DROP TABLE IF EXISTS ${table} CASCADE`,

			`CREATE TABLE ${table} (
				${_.keys(rows[0])
					.map(column => `${column} ${columnTypeFromName(column)}`).join(', ')},
				CONSTRAINT ${table}_pkey PRIMARY KEY (id)
			) WITH ( OIDS=FALSE )`
		]

		// Insert max 500 rows per query
		installQueries = installQueries.concat(_.chunk(rows, 500).map(rowShard => {
			var valueCount = 0
			return [`INSERT INTO ${table}
					(${_.keys(rowShard[0]).join(', ')})
				 VALUES	${rowShard.map(row =>
					`(${_.map(row, () => '$' + ++valueCount).join(', ')})`).join(', ')}`,
			 _.flatten(rowShard.map(row => _.values(row))) ]
		}))

		installQueries.push(`ANALYZE ${table}`)

		return querySequence(installQueries)
	}))

}

