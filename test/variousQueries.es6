var _ = require('lodash')

var querySequence         = require('./helpers/querySequence')
var scoresLoadFixture     = require('./fixtures/scoresLoad')
var variousQueriesFixture = require('./fixtures/variousQueries')

exports.variousQueries = function(test) {
	// Run each case in parallel
	Promise.all(_.map(variousQueriesFixture.cases, (details, caseId) =>
		new Promise((resolve, reject) => {
			printDebug && console.log('BEGINNING VARIOUS QUERY', caseId)

			// Modify table names in fixture data
			var fixtureData = _.zipObject(
				_.keys(variousQueriesFixture.data).map(table => `${table}_${caseId}`),
				_.values(variousQueriesFixture.data)
			)

			// Modify table names in query
			var query = applyTableSuffixes(details.query, caseId)

			scoresLoadFixture.install(fixtureData)
				.catch(error => console.error(error))
				.then(result => {
					var updateLog  = [] // Cache for any updates to this query
					var nextLogPos = 0 // Length at last action performed
					var select     = liveDb.select(query).on('update',
						(diff, data) => updateLog.push({ diff, data }))

					// For each event, check values or perform action, then continue
					var processEvents = (callback, index) => {
						index = index || 0

						// Check if at end of event list
						if(index === details.events.length) return callback()

						var event = details.events[index]

						_.forOwn(event, (data, eventType) => {
							printDebug && console.log('EVENT', eventType, updateLog.length)

							switch(eventType){
								case 'perform':
									nextLogPos = updateLog.length

									var queries =
										data.map(query => applyTableSuffixes(query, caseId))

									querySequence(queries).then(results => {
										// Move to next event
										processEvents(callback, index + 1)
									}, reject)
									break
								case 'stop':
									select.stop()
									processEvents(callback, index + 1)
								case 'diff':
								case 'data':
									if(updateLog.length === nextLogPos) {
										// No update yet since action
										setTimeout(() => {
											processEvents(callback, index)
										}, 100)
									}
									else {
										// New update has arrived, check against data or diff
										test.deepEqual(
											orderByIndex(updateLog[nextLogPos][eventType]), data,
											`${caseId} Difference on event #${nextLogPos}`)

										// Move to next event
										processEvents(callback, index + 1)
									}
									break
								case 'unchanged':
									setTimeout(() => {
										test.equal(updateLog.length, nextLogPos,
											`${caseId} Unexpected update on unchanged #${nextLogPos}:
												${JSON.stringify(updateLog[updateLog.length - 1])}`)

										// Move to next event
										processEvents(callback, index + 1)
									}, data)
									break
								default:
									throw new Error('Invalid event type: ' + eventType)
									break
							}
						})
					}
					processEvents(() => {
						select.stop()
						resolve()
					})
				})
		})
	)).then(() => test.done())
}

function orderByIndex(data) {
	if(data instanceof Array){
		return _.sortBy(data, '_index')
	}else if(data === null){
		return null
	}else{
		// Object with arrays as values
		return _.object(_.map(data, (value, key) => [ key, orderByIndex(value) ]))
	}
}

function applyTableSuffixes(originalQuery, suffix) {
	return _.keys(variousQueriesFixture.data)
		.map(table => {
			return { find: table, replace: `${table}_${suffix}` }
		})
		.reduce((query, table) => {
			return query.replace(new RegExp(table.find, 'g'), table.replace)
		}, originalQuery)
}
