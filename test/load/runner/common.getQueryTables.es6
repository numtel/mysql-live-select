var _ = require('lodash')

var common = require('../../../src/common')

module.exports = async function() {
	var conn = await common.getClient(options.conn)

	var tables = await common.getQueryTables(conn.client, settings.query)

	if(_.xor(tables, settings.expected).length !== 0) {
		console.error('UNEXPECTED RESULT', tables)
	}

	conn.done()
}

