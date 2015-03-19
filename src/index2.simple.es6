var util = require('util')
var LiveSQL = require('./LiveSQL')

const CONN_STR = 'postgres://meteor:meteor@127.0.0.1/meteor'
const CHANNEL = 'ben_test'

var liveDb = new LiveSQL(CONN_STR, CHANNEL)

liveDb.select(`
	SELECT
		*
	FROM
		scores
	ORDER BY
		score DESC
`, (diff, rows) => {
	console.log(util.inspect(diff, { depth: null }), rows)
})

// Ctrl+C
process.on('SIGINT', () => {
	liveDb.cleanup().then(process.exit)
})

