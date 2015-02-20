/**
 * Describe cases against data to fed into fixtures/scoresLoad.es6 :: install()
 */
const UNCHANGED_WAIT = 1000;

exports.data = {
	assignments: [
		{ id:1, class_id:1, name: 'Assignment 1', value:64 },
		{ id:2, class_id:1, name: 'Assignment 2', value:29 },
		{ id:3, class_id:1, name: 'Assignment 3', value:57 }
	],
	students: [
		{ id:1, name: 'Student 1' },
		{ id:2, name: 'Student 2' },
		{ id:3, name: 'Student 3' }
	],
	scores: [
		{ id:1, assignment_id:1, student_id:1, score:52 },
		{ id:2, assignment_id:1, student_id:2, score:54 },
		{ id:3, assignment_id:1, student_id:3, score:28 },
	]
}

exports.cases = {}
/**
	Each case is described by an object with a query and a set of events
	Each case is provided with a clean data set, as described above
	exports.cases.<case identifier string> = {
		query: <SQL SELECT statement string>,
		events: [
			Each event is described by an object with a single property
			The key can be 'diff', 'perform', or 'unchanged'
			{ diff: <deepEqual to most recent update event diff> }
			{ perform: [<array of SQL queries>] }
			{ unchanged: <milliseconds to wait> }
		]
	}
 */
exports.cases.innerJoin = {
	query: `
		SELECT
			students.name  AS student_name,
			assignments.name,
			assignments.value,
			scores.score
		FROM
			scores
		INNER JOIN assignments ON
			(assignments.id = scores.assignment_id)
		INNER JOIN students ON
			(students.id = scores.student_id)
		WHERE
			assignments.class_id = 1
	`,
	events: [
		{ diff: [
			[ 'added', 0,
				{ student_name: 'Student 1', name: 'Assignment 1', value: 64, score: 52 } ],
			[ 'added', 1,
				{ student_name: 'Student 2', name: 'Assignment 1', value: 64, score: 54 } ],
			[ 'added', 2,
				{ student_name: 'Student 3', name: 'Assignment 1', value: 64, score: 28 } ]
		] },
		{ perform: [
			`INSERT INTO scores (id, assignment_id, student_id, score) VALUES
				(4, 2, 1, 25)`
		] },
		{ diff: [ [ 'added', 3,
			{ student_name: 'Student 1', name: 'Assignment 2', value: 29, score: 25 }
		] ] },
		{ perform: [
			// student_id does not exist, will not be in result set
			`INSERT INTO scores (id, assignment_id, student_id, score) VALUES
				(5, 2, 4, 25)`
		] },
		{ unchanged: UNCHANGED_WAIT },
		{ perform: [
			`UPDATE scores SET score = 21 WHERE id = 4`
		] },
		{ diff: [ [ 'changed', 3,
			{ student_name: 'Student 1', name: 'Assignment 2', value: 29, score: 25 },
			{ student_name: 'Student 1', name: 'Assignment 2', value: 29, score: 21 }
		] ] },
		{ perform: [
			`UPDATE students SET name = 'John Doe' WHERE id = 2`
		] },
		{ diff: [ [ 'changed', 1,
			{ student_name: 'Student 2', name: 'Assignment 1', value: 64, score: 54 },
			{ student_name: 'John Doe',  name: 'Assignment 1', value: 64, score: 54 }
		] ] },
		{ perform: [
			`DELETE FROM scores WHERE id = 4`
		] },
		{ diff: [ [ 'removed', 3,
			{ student_name: 'Student 1', name: 'Assignment 2', value: 29, score: 21 }
		] ] },
		{ perform: [
			// assignment with different class_id, no changes
			`INSERT INTO assignments (id, class_id, name, value) VALUES
				(4, 2, 'Another Class', 20)`,
			`INSERT INTO scores (id, assignment_id, student_id, score) VALUES
				(6, 4, 1, 15)`
		] },
		{ unchanged: UNCHANGED_WAIT },
		{ perform: [
			`UPDATE scores SET score = 19 WHERE id = 6`
		] },
		{ unchanged: UNCHANGED_WAIT },
	]
}

exports.cases.leftRightJoin = {
	query: `
		SELECT
			students.name  AS student_name,
			assignments.name,
			assignments.value,
			scores.score
		FROM
			scores
		RIGHT JOIN assignments ON
			(assignments.id = scores.assignment_id)
		LEFT JOIN students ON
			(students.id = scores.student_id)
		WHERE
			assignments.class_id = 1
	`,
	events: [
		{ diff: [
			[ 'added', 0,
				{ student_name: 'Student 1', name: 'Assignment 1', value: 64, score: 52 } ],
			[ 'added', 1,
				{ student_name: 'Student 2', name: 'Assignment 1', value: 64, score: 54 } ],
			[ 'added', 2,
				{ student_name: 'Student 3', name: 'Assignment 1', value: 64, score: 28 } ],
			[ 'added', 3,
				{ student_name: null       , name: 'Assignment 2', value: 29, score: null } ],
			[ 'added', 4,
				{ student_name: null       , name: 'Assignment 3', value: 57, score: null } ]
		] },
		{ perform: [
			`INSERT INTO scores (id, assignment_id, student_id, score) VALUES
				(4, 2, 1, 25)`
		] },
		{ diff: [
			[ 'changed', 3,
				{ student_name: null, name: 'Assignment 2', value: 29, score: null },
				{ student_name: 'Student 1', name: 'Assignment 2', value: 29, score: 25 } ]
		] },
		{ perform: [
			`INSERT INTO scores (id, assignment_id, student_id, score) VALUES
				(5, 2, 4, 25)`
		] },
		{ diff: [
			[ 'changed', 4,
				{ student_name: null, name: 'Assignment 3', value: 57, score: null },
				{ student_name: null, name: 'Assignment 2', value: 29, score: 25 } ],
			[ 'added', 5,
				{ student_name: null, name: 'Assignment 3', value: 57, score: null } ]
		] },
		{ perform: [
			`UPDATE scores SET score = 21 WHERE id = 4`
		] },
		{ diff: [
			[ 'changed', 3,
				{ student_name: 'Student 1', name: 'Assignment 2', value: 29, score: 25 },
				{ student_name: null, name: 'Assignment 2', value: 29, score: 25 } ],
			[ 'changed', 4,
				{ student_name: null, name: 'Assignment 2', value: 29, score: 25 },
				{ student_name: 'Student 1', name: 'Assignment 2', value: 29, score: 21 } ]
		] },
		{ perform: [
			`UPDATE students SET name = 'John Doe' WHERE id = 2`
		] },
		{ diff: [
			[ 'changed', 1,
				{ student_name: 'Student 2', name: 'Assignment 1', value: 64, score: 54 },
				{ student_name: 'John Doe', name: 'Assignment 1', value: 64, score: 54 } ]
		] },
		{ perform: [
			`DELETE FROM scores WHERE id = 4`
		] },
		{ diff: [
			[ 'changed', 4,
				{ student_name: 'Student 1', name: 'Assignment 2', value: 29, score: 21 },
				{ student_name: null, name: 'Assignment 3', value: 57, score: null } ],
			[ 'removed', 5,
				{ student_name: null, name: 'Assignment 3', value: 57, score: null } ]
		] },
		{ perform: [
			// assignment with different class_id, no changes
			`INSERT INTO assignments (id, class_id, name, value) VALUES
				(4, 2, 'Another Class', 20)`,
			`INSERT INTO scores (id, assignment_id, student_id, score) VALUES
				(6, 4, 1, 15)`
		] },
		{ unchanged: UNCHANGED_WAIT },
		{ perform: [
			`UPDATE scores SET score = 19 WHERE id = 6`
		] },
		{ unchanged: UNCHANGED_WAIT },
	]
}

exports.cases.fullJoin = {
	query: `
			SELECT
			assignments.name,
			assignments.value,
			scores.score
		FROM
			scores
		FULL JOIN assignments ON
			(assignments.id = scores.assignment_id)
	`,
	events: [
		{ diff: [
			[ 'added', 0, { name: 'Assignment 1', value: 64, score: 52 } ],
			[ 'added', 1, { name: 'Assignment 1', value: 64, score: 54 } ],
			[ 'added', 2, { name: 'Assignment 1', value: 64, score: 28 } ],
			[ 'added', 3, { name: 'Assignment 2', value: 29, score: null } ],
			[ 'added', 4, { name: 'Assignment 3', value: 57, score: null } ]
		] },
		{ perform: [
			`INSERT INTO scores (id, assignment_id, student_id, score) VALUES
				(4, 4, 1, 25)`
		] },
		{ diff: [
			[ 'changed', 3,
				{ name: 'Assignment 2', value: 29, score: null },
				{ name: null, value: null, score: 25 } ],
			[ 'changed', 4,
				{ name: 'Assignment 3', value: 57, score: null },
				{ name: 'Assignment 2', value: 29, score: null } ],
			[ 'added', 5, { name: 'Assignment 3', value: 57, score: null } ]
		] },
	]
}

exports.cases.max = {
	query: `
		SELECT
			MAX(scores.score)
		FROM
			scores
		INNER JOIN assignments ON
			(assignments.id = scores.assignment_id)
		WHERE
			assignments.class_id = 1
		GROUP BY
			assignments.class_id
	`,
	events: [
		{ diff: [
			[ 'added', 0, { max: 54 } ]
		] },
		{ perform: [
			`UPDATE scores SET score = 64 WHERE id = 1`
		] },
		{ diff: [
			[ 'changed', 0, { max: 54 }, { max: 64 } ]
		] },
	]
}

exports.cases.inExpression = {
	query: `
		SELECT
			scores.score IN (54) as is_54
		FROM
			scores
		INNER JOIN assignments ON
			(assignments.id = scores.assignment_id)
		WHERE
			assignments.class_id = 1
	`,
	events: [
		{ diff: [
			[ 'added', 0, { is_54: false } ],
			[ 'added', 1, { is_54: true } ],
			[ 'added', 2, { is_54: false } ]
		] },
		{ perform: [
			`UPDATE scores SET score = 64 WHERE id = 2`
		] },
		{ diff: [
			[ 'changed', 1, { is_54: true }, { is_54: false } ]
		] },
	]
}

exports.cases.allExpression = {
	query: `
		SELECT
			score < ALL (SELECT score FROM scores WHERE score > 28) is_lte_28
		FROM
			scores
		INNER JOIN assignments ON
			(assignments.id = scores.assignment_id)
		WHERE
			assignments.class_id = 1
	`,
	events: [
		{ diff: [
			[ 'added', 0, { is_lte_28: false } ],
			[ 'added', 1, { is_lte_28: false } ],
			[ 'added', 2, { is_lte_28: true } ]
		] },
		{ perform: [
			`UPDATE scores SET score = 14 WHERE id = 2`
		] },
		{ diff: [
			[ 'changed', 1, { is_lte_28: false }, { is_lte_28: true } ]
		] },
	]
}

