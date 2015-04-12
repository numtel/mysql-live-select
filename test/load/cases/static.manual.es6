module.exports = {
	init: {
		classCount: 500,
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
		insert: 50,
		update: 50,
	}
}

