var _ = require('lodash')

var common = require('../../../src/common')

module.exports = async function() {
	var handle = await common.getClient(options.conn)
	await delay(1)
	handle.done()
	handle = null
}

function delay(duration) {
	return new Promise(resolve => setTimeout(resolve, duration))
}

