var _ = require('lodash')

var randomString = require('random-strings')
var querySequence = require('./helpers/querySequence')

var scoresLoadFixture = require('./fixtures/scoresLoad')

exports.scoresLoad = function(test) {
	var classCount =
		process.env.CLASS_COUNT ? parseInt(process.env.CLASS_COUNT) : 1
	var selectsPerClass =
		process.env.CLIENT_MULTIPLIER ? parseInt(process.env.CLIENT_MULTIPLIER) : 1
	var percentToUpdate =
		process.env.PERCENT_TO_UPDATE ? parseInt(process.env.PERCENT_TO_UPDATE) : 100
	var assignPerClass =
		process.env.ASSIGN_PER_CLASS ? parseInt(process.env.ASSIGN_PER_CLASS) : 4

	// Collect and report memory usage information
	if(printStats){
		var memoryUsageSnapshots = []
		var updateMemoryUsage = function() {
			memoryUsageSnapshots.push({
				time: Date.now(),
				memory: process.memoryUsage().heapTotal
			})
		}
		var memoryInterval = setInterval(updateMemoryUsage, 500)
	}


	var fixtureData = scoresLoadFixture.generate(
		classCount,
		assignPerClass,
		20,         // students per class
		6           // classes per student
	)
	// Generate new names to update to
	var newStudentNames = fixtureData.students.map(student =>
		randomString.alphaLower(10))

	printStats && console.log(
		'Students.length: ', fixtureData.students.length,
		'Assignments.length: ', fixtureData.assignments.length,
		'Scores.length: ', fixtureData.scores.length
	)

	printDebug && console.log('FIXTURE DATA\n', fixtureData)

	printStats && console.time('score data install')
	scoresLoadFixture.install(fixtureData).then(result => {
		printStats && console.timeEnd('score data install')

		var curStage = 0
		var liveSelects = []
		_.range(selectsPerClass).forEach(selectIndex => {
			liveSelects = liveSelects.concat(_.range(classCount).map(index =>
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
				`, [ index + 1 ]).on('update', diff => {
					switch(curStage){
						case 0:
							readyCount++
							initialData[index] = diff.added

							if(readyCount === liveSelects.length){
								printDebug && console.log('INITIAL UPDATE\n', initialData)

								curStage++
								readyCount = 0

								// May happen before or after ready
								updateStudentNames()
							}
							break
						case 1:
							readyCount++
							test.ok(diff.added
								.map(change =>
									change.student_name ===
										newStudentNames[change.student_id - 1])
								.indexOf(false) === -1, 'Student name update check')

							if(readyCount === liveSelects.length){
								printStats && console.time('student names changed on instances')
								curStage++
								readyCount = 0

								updateScores()
							}
							break
						case 2:
							diff.added.forEach(change => {
								if(change.score ===
										fixtureData.scores[change.score_id - 1].score * 2) {
									_.pull(updatedScoreIds, change.score_id)
								}
							})

							if(updatedScoreIds.length === 0){
								if(printStats){
									console.timeEnd('scores updated on instances')
									clearInterval(memoryInterval)
									printDebug && console.log(JSON.stringify(memoryUsageSnapshots))
								}
								// Make sure all are stopped
								liveSelects.forEach(thisSelect => thisSelect.stop())
								test.done()
							}
							break
					}
				})))
		})

		// Stage 0 : cache initial data
		var initialData = []
		var readyCount  = 0

		// Stage 1 : update each student name
		var updateStudentNames = function() {
			printStats && console.time('update student names')
			querySequence(newStudentNames.map((name, index) =>
				[ `UPDATE students SET name = $1 WHERE id = ${index + 1}`,
					[ name ] ])).then(result => {
					if(printStats) {
						console.timeEnd('update student names')
						console.time('student names changed on instances')
					}
				})

			// Only perform this operation once
			updateStudentNames = function() {}
		}

		// Stage 2 : update scores individually
		var updatedScoreIds = []
		var updateScores = function() {
			var scoresToUpdate = _.range(fixtureData.scores.length)
			var queries        = []
			var finalCount =
				Math.round(scoresToUpdate.length * (100 - percentToUpdate) / 100)

			while(scoresToUpdate.length > finalCount){
				var id = scoresToUpdate.splice(
					Math.floor(Math.random() * scoresToUpdate.length), 1)[0] + 1

				updatedScoreIds.push(id)

				queries.push([
					`UPDATE scores SET score = $1 WHERE id = $2`,
					[ fixtureData.scores[id - 1].score * 2, id ]
				])
			}

			printStats && console.time('score update queries')

			querySequence(queries).then(result => {
				if(printStats) {
					console.timeEnd('score update queries')
					console.time('scores updated on instances')
				}
			})
		}

	}, error => console.error(error))

}
