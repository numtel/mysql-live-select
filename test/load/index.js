/**
 * Load Test #2
 */

// ES6+ may be used in all files required by this one
require('babel/register')({ playground: true })

var _ = require('lodash')
var parseArgs = require('minimist')

// Determine options from command line arguments
var args = parseArgs(process.argv.slice(2))

var defaults = {
	conn    : 'postgres://meteor:meteor@127.0.0.1/meteor_test',
	channel : 'load_test',
	case    : 'static'
}

if(args.help === true){
	console.log('Load Test Runner\n')
	console.log('Default options:\n')
	console.log(defaults)
	console.log('\nUse \'--key="value"\' command line arguments to change defaults.')
	process.exit()
}

global.options = _.object(_.map(defaults, function(value, key) {
	return [ key, key in args ? args[key] : value ]
}))

global.settings = require('./cases/' + options.case)

// Setup in ES6 file
require('./setup')
