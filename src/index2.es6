var util = require('util');
var PgTriggers = require('./PgTriggers');

const CONN_STR = 'postgres://meteor:meteor@127.0.0.1/meteor';
const CHANNEL = 'ben_test';

var triggers = new PgTriggers(CONN_STR, CHANNEL);

triggers.on('error', err => console.log(err));

var mySelect = triggers.select(`
	SELECT
		students.name  AS student_name,
		students.id    AS student_id,
		assignments.id AS assignment_id,
		scores.id      AS score_id,
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
		assignments.class_id = $1
	ORDER BY
		score DESC
`, [ 1 ]);

mySelect.on('update', (diff, rows) => {
	console.log(util.inspect(diff, { depth: null }), rows);
});


process.on('SIGINT', function() {
	// Ctrl+C
	triggers.cleanup().then(process.exit)
});
