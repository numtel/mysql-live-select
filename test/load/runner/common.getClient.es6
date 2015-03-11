var _ = require('lodash')

var common = require('../../../src/common')

// Milliseconds to wait between closing one connection and opening another
const SLEEP_DURATION = 10

function openAndClose() {
	return new Promise((resolve, reject) => {
		common.getClient(options.conn).then(handle => {
			handle.done()
			resolve()
		}, reject)
	})
}

function openAndCloseForever() {
	openAndClose().then(() => {
		setTimeout(() => {
			process.stdout.write(['NEXT_EVENT', Date.now()].join(' '))
			openAndCloseForever()
		}, SLEEP_DURATION)
	}, reason => console.error('getClient Failed', reason))
}

_.range(settings.clientCount).map(openAndCloseForever)
