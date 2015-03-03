module.exports = {
	init: {
		classCount: 20,
		assignPerClass: 30,
		studentsPerClass: 20,
		classesPerStudent: 6
	},
	// Set a value lower than classCount to not have a LiveSelect for each class
	maxSelects: 50,
	// TODO multiplier may cause issues at this point!
	instanceMultiplier: 1,
	opPerSecond: {
		insert: 50,
		update: 50,
		// TODO deletes not described in diff.added so not used
		delete: 5
	}
}

