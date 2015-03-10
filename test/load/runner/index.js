require('babel/register')({ playground: true })

global.options  = JSON.parse(process.argv[2])
global.settings = JSON.parse(process.argv[3])

var runner = require(
	settings.customRunner ? './' + settings.customRunner : './LiveSQL.select')

setInterval(function() {
	var mem = process.memoryUsage()
	process.stdout.write([
		'MEMORY_USAGE',
		Date.now(),
		mem.heapTotal,
		mem.heapUsed,
	].join(' '))
}, 500)

