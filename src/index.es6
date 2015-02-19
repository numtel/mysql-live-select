/**
 * Example application
 */
var EventEmitter = require('events').EventEmitter;
var pg           = require('pg');
var _            = require('lodash');

var PgTriggers    = require('./PgTriggers');
var querySequence = require('./querySequence');

const CONN_STR = 'postgres://meteor:meteor@127.0.0.1/meteor';

var connect = function(cb) {
  return pg.connect(CONN_STR, cb);
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
var triggers     = new PgTriggers(connect, 'test_channel');

connect((error, client, done) => {
  if(error) throw error;

  cleanupDone = done;

  client.query(`TRUNCATE scores`, (error, result) => {
    // Create a trigger manager for this connection
    // Each connection should run on its own unique channel (2nd arg)
    for(var i = 0; i < 10; i++) {
      scores[i] = new liveClassScores(triggers, 1, i * 10);

      scores[i].on('update', function(i, changes) {
        console.log(i);

        changes.forEach(change => {
          if(change[0] === 'changed') {
            console.log({
              old : change[2],
              new : change[3]
            });
          }
        });

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

      for(var i = 0; i < 100; i++) {
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
          setTimeout(function() {
            client.query(`UPDATE scores SET score=TRUNC(RANDOM() * 99 + 1)`, (error, result) => {
              console.log(`updated ${result.rowCount} scores.`);
              done();
            })
          }, 2000);
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
  triggers.cleanup((error, results) => {
    if(error) throw error;

    if(cleanupDone) {
      cleanupDone();
    }

    process.exit();
  });
});
