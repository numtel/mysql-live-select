var common = require('../src/common')

const CONN_STR = process.env.CONN

var cases = {
	async getClient() {
		await common.getClient(CONN_STR)

	}
}

exports.
