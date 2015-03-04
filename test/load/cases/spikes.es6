// 50 op/sec for first second, then cycle up to 51 op/sec
var spikeOperations = elapsed =>
	elapsed < 1 ? 50 : (Math.sin(elapsed) * 50) + 1;

module.exports = {
	init: {
		classCount: 200,
		assignPerClass: 30,
		studentsPerClass: 20,
		classesPerStudent: 6
	},
	// Set a value lower than classCount to not have a LiveSelect for each class
	maxSelects: 30,
	// TODO multiplier may cause issues at this point!
	instanceMultiplier: 1,
	opPerSecond: {
		insert: spikeOperations,
		update: spikeOperations
	}
}

