var _ = require('lodash')
var sqlParser = require('sql-parser')

var common = require('../../../src/common')

var selectCount = 
	settings.maxSelects && settings.maxSelects < settings.init.classCount ?
		settings.maxSelects : settings.init.classCount

var currentData = require('../../fixtures/getDiffFromSupplied.currentData')
var notifications = require('../../fixtures/getDiffFromSupplied.notifications')

var query = `
	SELECT * FROM scores WHERE assignment_id IN (${
		_.range(settings.init.assignPerClass).map(i => '$' + (i + 1)).join(', ')
	}) ORDER BY id ASC`

var parsed = sqlParser.parse(query.replace(/\t/g, ' '))

var classId = 1

module.exports = async function() {
	var params = _.range(settings.init.assignPerClass).map(i =>
			(i * settings.init.classCount) + classId)

	// Client is empty object because it should not be used
	var update = await common.getDiffFromSupplied(
		{}, currentData, notifications, query, parsed, params)

	// Loop through active classes
	if(classId === selectCount) classId = 1
	else classId++
}

