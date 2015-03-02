module.exports = {
	init: {
		classCount: 10,
		assignPerClass: 30,
		studentsPerClass: 20,
		classesPerStudent: 6
	},
	// TODO multiplier may cause issues at this point!
	instanceMultiplier: 1,
	opPerSecond: {
		insert: 50,
		update: 50,
		// TODO deletes not described in diff.added so not used
		delete: 5
	}
}

