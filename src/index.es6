/**
 * Example application
 */
var EventEmitter = require('events').EventEmitter;
var pg           = require('pg');
var _            = require('lodash');

var PgTriggers = require('./PgTriggers');
var querySequence = require('./querySequence');

const CONN_STR = 'postgres://meteor:meteor@127.0.0.1/meteor';

var connect = function(callback) {
  return pg.connect(CONN_STR, callback);
}

var cleanupDone = null;

class liveClassScores extends EventEmitter {
  constructor(triggers, classId, score) {
    if(typeof triggers !== 'object' || typeof triggers.select !== 'function')
      throw new Error('first argument must be trigger manager object');
    if(typeof classId !== 'number')
      throw new Error('classId must be integer');

    var mySelect = triggers.select(`
      SELECT
        students.name  AS student_name,
        students.id    AS student_id,
        assignments.id AS assignment_id,
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
        assignments.class_id = $1 AND scores.score BETWEEN $2 AND $2 + 10
    `, [classId, score]);

    mySelect.on('update', (results, allRows) => {
      this.emit('update', results, allRows);
    });

    mySelect.on('ready', (results) => {
      this._ready = true;
      this.emit('ready', results);
    });
  }
}

var triggers  = [];
var scores    = [];

console.time('initial adds (offset 1s)');

function end() {
  console.timeEnd('initial adds (offset 1s)');
}

var throttledEnd = _.throttle(end, 1000, { leading : false });

connect((error, client, done) => {
  if(error) throw error;

  cleanupDone = done;

  client.query(`TRUNCATE scores`, (error, result) => {
    // Create a trigger manager for this connection
    // Each connection should run on its own unique channel (2nd arg)
    for(var i = 0; i < 1; i++) {
      triggers[i] = new PgTriggers(client, `test${i}`);
      scores[i]   = new liveClassScores(triggers[i], 1, i * 10);

      scores[i].on('update', function(i, results, allRows) {
        console.log(results);
        throttledEnd();
      }.bind(this, i));

      scores[i].on('ready', (i) => {
        var ready = !scores.filter((tmpScores) => !tmpScores._ready).length;

        if(ready) {
          test();
        }
      }.bind(this, i));
    }
  });
});

function test() {
  // Get some values to use
  var init = [
    `SELECT id FROM students`,
    `SELECT id FROM assignments`
  ];

  var studentIds    = [];
  var assignmentIds = [];

  connect((error, client, done) => {
    if(error) throw error;

    querySequence(client, init, (error, result) => {
      studentIds    = result[0].rows.map((row) => row.id);
      assignmentIds = result[1].rows.map((row) => row.id);

      var sql  = [];
      var rows = [];

      for(var i = 0; i < 10; i++) {
        var studentId    = choice(studentIds);
        var assignmentId = choice(assignmentIds);
        var score        = Math.ceil(Math.random() * 100);

        rows.push(`
            (${studentId}, ${assignmentId}, ${score})
        `);
      }

      var sql = `
        INSERT INTO scores
          (student_id, assignment_id, score)
        VALUES
          ${rows.join(", ")}
      `;

      client.query(sql, (error, result) => {
        if(error) return console.log(error);
        client.query(`SELECT COUNT('') AS count FROM scores`, (error, result) => {
          console.log(`inserted ${result.rows.pop().count} scores.`);
          done();
        });
      });
    });
  });
}

function choice(items) {
  var i = Math.floor(Math.random() * items.length);
  return items[i];
}

process.on('SIGINT', function() {
  // Ctrl+C
  for(var i in triggers) {
    triggers[i].stop((i, error, results) => {
      if(error) throw error;

      triggers[i]._done = true;

      if(!triggers.filter((trigger) => !trigger._done).length) {
        if(cleanupDone) {
          cleanupDone();
        }

        process.exit();
      }
    }.bind(this, i));
  }
});
