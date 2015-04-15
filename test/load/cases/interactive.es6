var rate = 10

module.exports = {
	init: {
		classCount: 200,
		assignPerClass: 30,
		studentsPerClass: 20,
		classesPerStudent: 6
	},
	customRunner: 'LivePG.select.manual',
	// Set a value lower than classCount to not have a LiveSelect for each class
	maxSelects: 50,
	// TODO multiplier may cause issues at this point!
	instanceMultiplier: 1,
	opPerSecond: {
		insert: elapsed => rate,
		update: elapsed => rate,
	}
}

process.nextTick(function(){
	process.stdin.resume();
	process.stdin.setEncoding('utf8');
	process.stdin.on('error', function (err) {
		console.log(err);
		process.exit();
	});
	process.stdin.on('data', function (chunk) {
		if(chunk.trim().match(/^\d+$/) !== null) {
			rate = parseInt(chunk.trim(), 10)
		}
	});
});

process.openStdin()
process.stdin.pause()

