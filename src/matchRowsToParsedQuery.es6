
module.exports = function(rows, parsed, params) {
	if(parsed.where === null) {
		return rows
	}

	var selected = []
	for(let row of rows) {
		if(rowMatchWhereClause(row, parsed.where.conditions, params)) {
			selected.push(row)
		}
	}

	return selected
}

function rowMatchWhereClause(row, conditions, params) {
	let details
	let isList = () => {
		details = readListCondition(conditions, row, params) }
	let isCompare = () => {
		details = readCompareCondition(conditions, row, params) }

	switch(conditions.operation.toUpperCase()) {
		case 'AND':
			return rowMatchWhereClause(row, conditions.left, params)
				&& rowMatchWhereClause(row, conditions.right, params)
		case 'OR':
			return rowMatchWhereClause(row, conditions.left, params)
				|| rowMatchWhereClause(row, conditions.right, params)
		case 'IN':
			isList()
			return details.values.indexOf(row[details.column]) !== -1
		case 'NOT IN':
			isList()
			return details.values.indexOf(row[details.column]) === -1
		case '=':
		case 'IS':
			isCompare()
			return details.left === details.right
		case '!=':
		case 'IS NOT':
		case '<>':
			isCompare()
			return details.left !== details.right
		case '>=':
			isCompare()
			return details.left >= details.right
		case '>':
			isCompare()
			return details.left > details.right
		case '<=':
			isCompare()
			return details.left <= details.right
		case '<':
			isCompare()
			return details.left < details.right
		case 'LIKE':
			isCompare()
			let regex = new RegExp(
				details.right.replace(/_/g, '.').replace(/%/g, '[\\s\\S]+'))
			return !! String(details.left).match(regex)
		default:
			throw new Error('INVALID_OPERATION: ' + conditions.operation)
	}
}

function parsedIsIdentifier(details) {
	return details.constructor.name === 'LiteralValue'
		&& 'value2' in details
}

function readListCondition(condition, row, params) {
	let column, values

	for(let side of ['left', 'right']) {
		if(parsedIsIdentifier(condition[side])) {
			column = condition[side].value
		}
		else if(condition[side].constructor.name === 'ListValue') {
			values = condition[side].value.map(listItem =>
				readValue(listItem, row, params))
		}
	}

	if(!column || !values)
		throw new Error('INVALID_LIST_CONDITION')
	if(!(column in row))
		throw new Error('MISSING_COLUMN_IN_ROW: ' + column)

	return { column, values }
}

function readCompareCondition(condition, row, params) {
	return {
		left: readValue(condition.left, row, params),
		right: readValue(condition.right, row, params)
	}
}

function readValue(instance, row, params) {
	switch(instance.constructor.name) {
		case 'LiteralValue':
			if('value2' in instance) {
				// Is a column name identifier, read value from column
				if(!(instance.value in row))
					throw new Error('MISSING_COLUMN_IN_ROW: ' + instance.value)

				return row[instance.value]
			}
			// Is a literal number value
			return instance.value
		case 'StringValue':
			return instance.value
		case 'ParameterValue':
			if(!params || !(instance.index in params))
				throw new Error('PARAMETER_MISSING: ' + instance.value)
			return params[instance.index]
		default:
			throw new Error('INVALID_VALUE')
	}
}

