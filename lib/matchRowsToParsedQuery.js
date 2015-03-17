"use strict";

var _core = require("babel-runtime/core-js")["default"];

module.exports = function (rows, parsed, params) {
	if (parsed.where === null) {
		return rows;
	}

	var selected = [];
	var _iteratorNormalCompletion = true;
	var _didIteratorError = false;
	var _iteratorError = undefined;

	try {
		for (var _iterator = _core.$for.getIterator(rows), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
			var row = _step.value;

			if (rowMatchWhereClause(row, parsed.where.conditions, params)) {
				selected.push(row);
			}
		}
	} catch (err) {
		_didIteratorError = true;
		_iteratorError = err;
	} finally {
		try {
			if (!_iteratorNormalCompletion && _iterator["return"]) {
				_iterator["return"]();
			}
		} finally {
			if (_didIteratorError) {
				throw _iteratorError;
			}
		}
	}

	return selected;
};

function rowMatchWhereClause(_x, _x2, _x3) {
	var _left;

	var _again = true;

	_function: while (_again) {
		_again = false;
		var row = _x,
		    conditions = _x2,
		    params = _x3;
		details = isList = isCompare = regex = undefined;

		var details = undefined;
		var isList = function () {
			details = readListCondition(conditions, row, params);
		};
		var isCompare = function () {
			details = readCompareCondition(conditions, row, params);
		};

		switch (conditions.operation.toUpperCase()) {
			case "AND":
				if (!(_left = rowMatchWhereClause(row, conditions.left, params))) {
					return _left;
				}

				_x = row;
				_x2 = conditions.right;
				_x3 = params;
				_again = true;
				continue _function;

			case "OR":
				if (_left = rowMatchWhereClause(row, conditions.left, params)) {
					return _left;
				}

				_x = row;
				_x2 = conditions.right;
				_x3 = params;
				_again = true;
				continue _function;

			case "IN":
				isList();
				return details.values.indexOf(row[details.column]) !== -1;
			case "NOT IN":
				isList();
				return details.values.indexOf(row[details.column]) === -1;
			case "=":
			case "IS":
				isCompare();
				return details.left === details.right;
			case "!=":
			case "IS NOT":
			case "<>":
				isCompare();
				return details.left !== details.right;
			case ">=":
				isCompare();
				return details.left >= details.right;
			case ">":
				isCompare();
				return details.left > details.right;
			case "<=":
				isCompare();
				return details.left <= details.right;
			case "<":
				isCompare();
				return details.left < details.right;
			case "LIKE":
				isCompare();
				var regex = new RegExp(details.right.replace(/_/g, ".").replace(/%/g, "[\\s\\S]+"));
				return !!String(details.left).match(regex);
			default:
				throw new Error("INVALID_OPERATION: " + conditions.operation);
		}
	}
}

function parsedIsIdentifier(details) {
	return details.constructor.name === "LiteralValue" && "value2" in details;
}

function readListCondition(condition, row, params) {
	var column = undefined,
	    values = undefined;

	var _iteratorNormalCompletion = true;
	var _didIteratorError = false;
	var _iteratorError = undefined;

	try {
		for (var _iterator = _core.$for.getIterator(["left", "right"]), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
			var side = _step.value;

			if (parsedIsIdentifier(condition[side])) {
				column = condition[side].value;
			} else if (condition[side].constructor.name === "ListValue") {
				values = condition[side].value.map(function (listItem) {
					return readValue(listItem, row, params);
				});
			}
		}
	} catch (err) {
		_didIteratorError = true;
		_iteratorError = err;
	} finally {
		try {
			if (!_iteratorNormalCompletion && _iterator["return"]) {
				_iterator["return"]();
			}
		} finally {
			if (_didIteratorError) {
				throw _iteratorError;
			}
		}
	}

	if (!column || !values) throw new Error("INVALID_LIST_CONDITION");
	if (!(column in row)) throw new Error("MISSING_COLUMN_IN_ROW: " + column);

	return { column: column, values: values };
}

function readCompareCondition(condition, row, params) {
	return {
		left: readValue(condition.left, row, params),
		right: readValue(condition.right, row, params)
	};
}

function readValue(instance, row, params) {
	switch (instance.constructor.name) {
		case "LiteralValue":
			if ("value2" in instance) {
				// Is a column name identifier, read value from column
				if (!(instance.value in row)) throw new Error("MISSING_COLUMN_IN_ROW: " + instance.value);

				return row[instance.value];
			}
			// Is a literal number value
			return instance.value;
		case "StringValue":
			return instance.value;
		case "ParameterValue":
			if (!params || !(instance.index in params)) throw new Error("PARAMETER_MISSING: " + instance.value);
			return params[instance.index];
		default:
			throw new Error("INVALID_VALUE");
	}
}