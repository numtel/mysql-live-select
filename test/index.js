/**
 * Test Intialization
 */
if(!('CONN' in process.env))
	throw new Error(
		'CONN environment variable required! (database connection string)')
if(!('CHANNEL' in process.env))
	throw new Error(
		'CHANNEL environment variable required! (notification identifier string)')

// Global flags
global.printDebug = process.env.DEBUG === '1'
global.printStats = process.env.STATS === '1'
global.isLoadTest = process.env.LOAD_TEST === '1'

// ES6+ may be used in all files required by this one
require('babel/register')({ playground: true })

var _       = require('lodash')
var LiveSQL = require('../')

// Define global instance
global.liveDb   = new LiveSQL(process.env.CONN, process.env.CHANNEL)

if(isLoadTest) {
	module.exports = _.assign(
		require('./helpers/lifecycle'),
		require('./scoresLoad')
	)
}
else{
	// Load full test suite
	module.exports = _.assign(
		require('./helpers/lifecycle'),
// 		require('./scoresLoad'), // Optional CLASS_COUNT env variable, default 1
// 		require('./variousQueries'),
		require('./matchRowsToParsedQuery')
	)
}
