var _         = require('lodash')
var sqlParser = require('sql-parser')

var matchRows = require('../src/matchRowsToParsedQuery')

var sampleRows = [
	{ _index: 1, id: 1, cat: 100, dog: 'Grey', tail: 'Spotted' },
	{ _index: 2, id: 2, cat: 200, dog: 'Brown', tail: 'Brown' },
	{ _index: 3, id: 3, cat: 300, dog: 'Tan', tail: 'Long' }
]

function matchCase(expectedIndexes, query, params) {
	return (test) => {
		var parsed = sqlParser.parse(query)
		var matched = matchRows(sampleRows, parsed, params)
		test.deepEqual(matched.map(row => row._index), expectedIndexes)
		test.done()
	}
}

exports.nullWhere = matchCase([ 1, 2, 3 ],
	`SELECT * FROM petlandia`)

exports.inClause = matchCase([ 2, 3 ],
	`SELECT * FROM petlandia WHERE dog IN ('Brown', 'Tan')`)

exports.notInClause = matchCase([ 1 ],
	`SELECT * FROM petlandia WHERE dog NOT IN ('Brown', 'Tan')`)

exports.compareEqual = matchCase([ 2 ],
	`SELECT * FROM petlandia WHERE cat = 200`)

exports.compareIs = matchCase([ 2 ],
	`SELECT * FROM petlandia WHERE cat IS 200`)

exports.compareNotEqual = matchCase([ 1, 3 ],
	`SELECT * FROM petlandia WHERE cat != 200`)

exports.compareIsNot = matchCase([ 1, 3 ],
	`SELECT * FROM petlandia WHERE cat IS NOT 200`)

exports.compareGreaterAndLess = matchCase([ 1, 3 ],
	`SELECT * FROM petlandia WHERE cat <> 200`)

exports.compareGreaterEqual = matchCase([ 2, 3 ],
	`SELECT * FROM petlandia WHERE cat >= 200`)

exports.compareGreater = matchCase([ 3 ],
	`SELECT * FROM petlandia WHERE cat > 200`)

exports.compareLessEqual = matchCase([ 1, 2 ],
	`SELECT * FROM petlandia WHERE cat <= 200`)

exports.compareLess = matchCase([ 1 ],
	`SELECT * FROM petlandia WHERE cat < 200`)

exports.compareIdentifier = matchCase([ 2 ],
	`SELECT * FROM petlandia WHERE dog = tail`)

exports.compareParameter = matchCase([ 2 ],
	`SELECT * FROM petlandia WHERE cat = $1`, [ 200 ])

exports.compareLike = matchCase([ 2 ],
	`SELECT * FROM petlandia WHERE dog LIKE 'Br_w%'`)

exports.andClause = matchCase([ 2 ],
	`SELECT * FROM petlandia WHERE dog = 'Brown' AND cat < 300`)

exports.orClause = matchCase([ 2, 3 ],
	`SELECT * FROM petlandia WHERE dog = 'Brown' OR cat = 300`)

exports.multipleClauses = matchCase([ 1, 2, 3 ],
	`SELECT * FROM petlandia WHERE dog = 'Brown' OR cat = 300 OR dog = 'Grey'`)

exports.nestedClauses = matchCase([ 1, 2 ],
	`SELECT * FROM petlandia WHERE (dog = 'Brown' AND tail = 'Brown') OR cat = 100`)

