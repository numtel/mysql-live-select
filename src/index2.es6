var PgTriggers = require('./PgTriggers');

const CONN_STR = 'postgres://meteor:meteor@127.0.0.1/meteor';
const CHANNEL = 'ben_test';

var triggers = new PgTriggers(CONN_STR, CHANNEL);

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
`, [ 1 ]);

mySelect.on('update', diff => {
	console.log(diff);
});


process.on('SIGINT', function() {
	// Ctrl+C
	triggers.cleanup().then(process.exit)
});
