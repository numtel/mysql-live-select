require('babel/register')({ playground: true })
var _ = require('lodash')

global.options  = JSON.parse(process.argv[2])
global.settings = JSON.parse(process.argv[3])

var runner = require('./' + settings.customRunner)

if(typeof runner === 'function') {
	// Unit tests will export an async function that can be run over and over

	// Milliseconds to wait between finishing one operation and starting next
	var SLEEP_DURATION = 10

	var runAgain = function() {
		process.stdout.write(['NEXT_EVENT', Date.now()].join(' '))
		performOperationForever()
	}

	var runAfterTimeout = function() {
		setTimeout(runAgain, SLEEP_DURATION)
	}

	var runnerError = function(reason) {
		console.error('Operation Failed', reason.stack)
	}

	var performOperationForever = function() {
		runner().then(runAfterTimeout, runnerError)
	}

	_.range(settings.clientCount || 1).map(performOperationForever)
}

setInterval(function() {
	var mem = process.memoryUsage()
	process.stdout.write([
		'MEMORY_USAGE',
		Date.now(),
		mem.heapTotal,
		mem.heapUsed,
		liveDb ? liveDb.refreshCount : '0',
		liveDb ? liveDb.notifyCount : '0'
	].join(' '))
}, 500)

