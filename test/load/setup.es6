require('console.table');
var _ = require('lodash');
var pg = require('pg');
var babar = require('babar');
var stats = require('simple-statistics');
var spawn = require('child_process').spawn;

var querySequence = require('../../src/querySequence');
var scoresLoadFixture = require('../fixtures/scoresLoad');

const MIN_WAIT = 100;

var memoryUsage = [];
var classUpdates = [];
var waitingOps = [];

var selectCount = settings.maxSelects &&
	settings.maxSelects < settings.init.classCount ?
		settings.maxSelects : settings.init.classCount;

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
	'Instance Multiplier: ', settings.instanceMultiplier,
	'Select Count: ', selectCount, '\n\n',
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
			'--debug',
			'test/load/runner/',
			options.conn,
			options.channel,
			settings.init.classCount,
			settings.instanceMultiplier,
			settings.maxSelects
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

					if(classUpdates.length === selectCount) {
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
var performRandom = {
	update() {
		// Select random score record
		do {
			var scoreRow = fixtureData.scores[
				Math.floor(Math.random() * fixtureData.scores.length)];

			var classId = fixtureData.assignments[scoreRow.assignment_id - 1].class_id;
		} while(classId > settings.maxSelects);

		clientPromise.then(client => {
			// Record operation time
			waitingOps.push({
				scoreId: scoreRow.id,
				time: Date.now()
			});
// 	 		console.log('UPDATING', scoreRow.id, classId);

			client.query(
				'UPDATE scores SET score = score + 1 WHERE id = $1', [ scoreRow.id ])
		});
	},
	insert() {
		// Select random assignment and student ids
		do {
			var assignId =
				Math.floor(Math.random() * fixtureData.assignments.length) + 1;
			var classId = fixtureData.assignments[assignId - 1].class_id;
		} while(classId > settings.maxSelects);

		var studentId = Math.floor(Math.random() * fixtureData.students.length) + 1;

		var scoreId = fixtureData.scores.length + 1;

		clientPromise.then(client => {
			waitingOps.push({
				scoreId,
				time: Date.now()
			});
// 	 		console.log('INSERTING', scoreId, classId, assignId);

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
	}
};

childPromise.then(() => {
	console.log('\nLoad operations in progress...')

	// Print elapsed time every 15 seconds for easy duration checking
	var runTimeSeconds = 0;
	setInterval(() => {
		runTimeSeconds++;
		if(runTimeSeconds % 15 === 0){
			console.log(runTimeSeconds);
		}
	}, 1000);

	var startTime = Date.now();

	var getInterval = valueFun => valueFun((Date.now() - startTime) / 1000);

	_.forOwn(performRandom, (fun, key) => {
		var value = settings.opPerSecond[key];
		if(!value) return;

		switch(typeof value) {
			case 'number':
				// Static value provided
				setInterval(fun, Math.ceil(1000 / value))
				break;
			case 'function':
				// Determined based on elapsed time
				// Single argument receives (float) number of seconds elapsed
				var nextOp = () => {
					fun();
					setTimeout(nextOp, Math.ceil(1000 / getInterval(value)));
				}
				setTimeout(nextOp, Math.ceil(1000 / getInterval(value)));
				break;
		}
	});
});


process.on('SIGINT', () => {
	clientDone && clientDone();

	var filteredEvents = classUpdates.filter(evt => evt.responseTimes !== null)

	console.log(
		'Final Data Count \n',
		'Scores: ', fixtureData.scores.length, '\n',
		'Responses Received: ', filteredEvents.length, '\n',
		'Still Waiting for Response: ', waitingOps.length, '\n'
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

		var allResponseTimes = eventPrep.map(evt => evt[1]);
		console.table([ 0.05, 0.25, 0.5, 0.75, 0.95, 1 ].map(percentile => {
			return {
				'Percentile': percentile * 100,
				'Time (ms)': Math.round(stats.quantile(allResponseTimes, percentile))
			}
		}));
		
	}

	process.exit();
});

