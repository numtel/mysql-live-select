var _ = require('lodash');
var pg = require('pg');
var babar = require('babar');
var spawn = require('child_process').spawn;

var querySequence = require('../../src/querySequence');
var scoresLoadFixture = require('../fixtures/scoresLoad');

const MIN_WAIT = 100;

var memoryUsage = [];
var classUpdates = [];
var waitingOps = [];

// Create initial data
var fixtureData = scoresLoadFixture.generate(
	settings.init.classCount,
	settings.init.assignPerClass,
	settings.init.studentsPerClass,
	settings.init.classesPerStudent
);

console.log(
	'Press CTRL+C to exit and display results when satisfied with duration.\n\n',
	'Class Count: ', settings.init.classCount,
	'Instance Multiplier: ', settings.instanceMultiplier, '\n\n',
	'Initial Data Count \n',
	'Students: ', fixtureData.students.length,
	'Assignments: ', fixtureData.assignments.length,
	'Scores: ', fixtureData.scores.length, '\n\n',
	'Operation per Second \n',
	'Insert: ', settings.opPerSecond.insert,
	'Update: ', settings.opPerSecond.update, '\n'
);

var clientDone = null;
var clientPromise = new Promise((resolve, reject) => {
	console.time('Connected to database server');
	pg.connect(options.conn, (error, client, done) => {
		console.timeEnd('Connected to database server');
		if(error) return reject(error);
		clientDone = done;
		resolve(client);
	})
}).catch(reason => console.error(reason));

var installPromise = new Promise((resolve, reject) => {
	clientPromise.then(client => {
		console.time('Installed inital data');
		scoresLoadFixture.install(client, fixtureData).then(() => {
			console.timeEnd('Installed inital data');
			resolve()
		}, reject)
	}, reject)
}).catch(reason => console.error(reason));

// Spawn child process
var childPromise = new Promise((resolve, reject) => {
	installPromise.then(() => {
		console.time('Initialized each select instance');
		var child = spawn('node', [
			'test/load/runner/',
			options.conn,
			options.channel,
			settings.init.classCount,
			settings.instanceMultiplier
		]);

		child.stdout.on('data', data => {
			data = data.toString().split(' ');
			switch(data[0]) {
				case 'MEMORY_USAGE':
					memoryUsage.push({
						time: parseInt(data[1], 10),
						memory: parseInt(data[2], 10)
					});
					break;
				case 'CLASS_UPDATE':
					var eventTime = parseInt(data[1], 10);
					var scoreIds = data[2].split(',').map(scoreId => parseInt(scoreId, 10));
					var responseTimes = null;

					// TODO why does this sometimes output all rows!?
// 					classUpdates.length > settings.init.classCount &&
// 						console.log('UPin', scoreIds);

					if(waitingOps.length !== 0){
						var myOps = waitingOps.filter(op =>
							scoreIds.indexOf(op.scoreId) !== -1);
						// TODO why is there not 1:1 relationship?
						if(myOps.length !== 0){
							// Remove myOps from the global wait list
							waitingOps = waitingOps.filter(op =>
								scoreIds.indexOf(op.scoreId) === -1);

							// Calculate response time
							responseTimes = myOps.map(op => eventTime - op.time);
						}
					}

					classUpdates.push({
						time: eventTime,
						responseTimes
					});

					if(classUpdates.length === settings.init.classCount) {
						// childPromise is ready when all selects have initial data
						console.timeEnd('Initialized each select instance');
						resolve()
					}
					break;
				default:
					console.log('stdout', data);
					break;
			}
		});

		child.stderr.on('data', data => {
			console.log('stderr', data.toString());
		});

		child.on('close', code => {
			console.log('exited with code', code);
		});
	}, reject)
}).catch(reason => console.error(reason));

// Begin throttled test operations
var performRandomUpdate = function() {
	// Select random score record
	var scoreRow = fixtureData.scores[
		Math.floor(Math.random() * fixtureData.scores.length)];

	clientPromise.then(client => {
		// Record operation time
		waitingOps.push({
			scoreId: scoreRow.id,
			time: Date.now()
		});
// 		console.log('UPDATING', scoreRow.id);

		client.query(
			'UPDATE scores SET score = score + 1 WHERE id = $1', [ scoreRow.id ])
	});
};

var performRandomInsert = function() {
	// Select random assignment and student ids
	var assignId = Math.floor(Math.random() * fixtureData.assignments.length) + 1;
	var studentId = Math.floor(Math.random() * fixtureData.students.length) + 1;

	var scoreId = fixtureData.scores.length + 1;

	clientPromise.then(client => {
		waitingOps.push({
			scoreId,
			time: Date.now()
		});
// 		console.log('INSERTING', scoreId);

		fixtureData.scores.push({
			id: scoreId,
			assignment_id: assignId,
			student_id: studentId,
			score: 5
		});

		client.query(
			`INSERT INTO scores (id, assignment_id, student_id, score)
				VALUES ($1, $2, $3, $4)`, [ scoreId, assignId, studentId, 5 ])
	});
};

childPromise.then(() => {
	console.log('\nLoad operations in progress...')

	settings.opPerSecond.insert &&
		setInterval(performRandomInsert,
			Math.ceil(1000 / settings.opPerSecond.insert));

	settings.opPerSecond.update &&
		setInterval(performRandomUpdate,
			Math.ceil(1000 / settings.opPerSecond.update));
});


process.on('SIGINT', () => {
	clientDone && clientDone();

// 	console.log('STILL waiting', waitingOps);
	console.log(
		'Final Data Count \n',
		'Scores: ', fixtureData.scores.length, '\n'
	);

	if(memoryUsage.length !== 0){
		// Print memory usage graph
		var firstMemTime = memoryUsage[0].time;
		var memoryPrep = memoryUsage.map(record => [
			(record.time - firstMemTime) / 1000,
			Math.round(record.memory / Math.pow(1024, 2) * 10 ) / 10
		]);

		console.log(babar(memoryPrep, {
			caption: 'Memory Usage by Elapsed Time (Megabytes / Seconds)'
		}));
	}

	var filteredEvents = classUpdates.filter(evt =>
		evt.responseTimes !== null)
	if(filteredEvents.length !== 0){
		// Print response time graph
// 		console.log(filteredEvents);
		var eventPrep = filteredEvents.map(evt => [
			(evt.time - firstMemTime) / 1000,
			evt.responseTimes.reduce((prev, cur) => prev + cur, 0)
				/ evt.responseTimes.length
		]);

		console.log(babar(eventPrep, {
			caption: 'Response Time by Elapsed Time (Milliseconds / Seconds)'
		}));
	}

	process.exit();
});

