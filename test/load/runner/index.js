require('babel/register');

var _ = require('lodash');

global.connStr            = process.argv[2];
global.channel            = process.argv[3];
global.classCount         = parseInt(process.argv[4], 10);
global.instanceMultiplier = parseInt(process.argv[5], 10);
global.maxSelects         = parseInt(process.argv[6], 10);

var LiveSQL = require('../../../');

global.liveDb = new LiveSQL(connStr, channel);

liveDb.on('error', function(error) {
	console.error(error);
});

var selects = require('./classes');

setInterval(function() {
	process.stdout.write([
		'MEMORY_USAGE',
		Date.now(),
		process.memoryUsage().heapTotal
	].join(' '));
}, 500);

