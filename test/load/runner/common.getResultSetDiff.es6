var _ = require('lodash')

var common = require('../../../src/common')

var curData = []

module.exports = async function() {
	var conn = await common.getClient(options.conn)

	var update = await common.getResultSetDiff(
		conn.client, curData, settings.query, settings.params)

	// Put some rows into curData for next round
	if(update !== null && update.diff.added !== null) {
		curData = update.diff.added.filter(row => Math.random() > 0.5)
	}

	conn.done()
}

