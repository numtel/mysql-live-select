var _ = require('lodash')

var common = require('../../../src/common')

var curData = []

module.exports = async function() {
	var conn = await common.getClient(options.conn)

	var diff = await common.getResultSetDiff(
		conn.client, curData, settings.query, settings.params)

	// Put some rows into curData for next round
	if(diff.added !== null) {
		curData = diff.added.filter(row => Math.random() > 0.5)
	}

	conn.done()
}

