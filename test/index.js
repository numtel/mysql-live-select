/* mysql-live-select, MIT License ben@latenightsketches.com
   test/index.js - Test Suite */
var _ = require('lodash');
var LiveMysql = require('../');
var settings = require('./settings/mysql');
var querySequence = require('./helpers/querySequence');
var Connector = require('./helpers/connector');
var multipleQueriesData = require('./fixtures/multipleQueries');
var server = new Connector(settings);

module.exports = {
  setUp: function(done){
    server.testCount++;
    done();
  },
  tearDown: function(done){
    server.closeIfInactive(1000);
    done();
  },
  basic: function(test){
    var table = 'basic';
    //  *  Test that all events emit with correct arguments
    // [1] Test that duplicate queries are cached
    server.on('ready', function(conn, esc, escId, queries){
      querySequence(conn.db, [
        'DROP TABLE IF EXISTS ' + escId(table),
        'CREATE TABLE ' + escId(table) + ' (col INT UNSIGNED)',
      ], function(results){
        queries.splice(0, queries.length);

        var query = 'SELECT * FROM ' + escId(table);
        var conditionCheckIndex = 0;
        var triggers = [ {
          database: server.database,
          table: table,
          condition: function(row, newRow, isDeleted) {
            // Ensure that each call of this condition function receives
            // the correct arguments
            conditionCheckIndex++;
            switch(conditionCheckIndex) {
              case 1:
                // Row has been inserted
                test.equal(row.col, 10);
                test.equal(newRow, null);
                test.equal(isDeleted, false);
                break;
              case 2:
                // Row has been updated
                test.equal(row.col, 10);
                test.equal(newRow.col, 15);
                test.equal(isDeleted, null);
                break;
              case 3:
                // Row has been deleted
                test.equal(row.col, 15);
                test.equal(newRow, null);
                test.equal(isDeleted, true);
                break;
            }
            return true;
          }
        } ];
        // Second, resultsBuffer check query doesn't need the condition
        var triggersSimple = [ {
          database: server.database,
          table: table
        } ];

        conn.select(query, triggers).on('update', function(diff, data) {
          // After initial update
          if(data.length > 0 && data[0].col === 10){
            // Second select instance to check resultsBuffer
            var secondInstanceInitialized = false;
            conn.select(query, triggersSimple).on('update', function(diff, data) {
              if(secondInstanceInitialized === false) {
                // Check that the diff is correct when initializing from cache
                test.deepEqual(diff,
                  { removed: null,
                    moved: null,
                    copied: null,
                    added: [ { col: 10, _index: 1 } ] });
                secondInstanceInitialized = true;
              }

              if(data.length > 0 && data[0].col === 15){
                // [1] Test in LiveMysqlSelect created later,
                // Ensure only First select, update, second select occurred
                // Along with the INSERT and UPDATE queries, 5 total
                // i.e. No duplicate selects, resultsBuffer working
                test.equal(queries.length, 5);
                conn.db.query('DELETE FROM ' + escId(table));
              }
            });
          }

          switch(conditionCheckIndex) {
            case 0:
              // Initialized as empty
              test.equal(data.length, 0);
              test.deepEqual(diff,
                { removed: null, moved: null, copied: null, added: [] });
              break;
            case 1:
              // Row has been inserted
              test.equal(data[0].col, 10);
              test.equal(data.length, 1);
              test.deepEqual(diff,
                 { removed: null,
                   moved: null,
                   copied: null,
                   added: [ { col: 10, _index: 1 } ] });
              break;
            case 2:
              // Row has been updated
              test.equal(data[0].col, 15);
              test.equal(data.length, 1);
              test.deepEqual(diff,
                 { removed: [ { _index: 1 } ],
                   moved: null,
                   copied: null,
                   added: [ { col: 15, _index: 1 } ] });
              break;
            case 3:
              // Row has been deleted
              test.equal(data.length, 0);
              test.deepEqual(diff,
                 { removed: [ { _index: 1 } ],
                   moved: null,
                   copied: null,
                   added: null });
              test.done();
              break;
          }
        });

        // Perform database operation sequence
        querySequence(conn.db, [
          'INSERT INTO ' + escId(table) + ' (col) VALUES (10)'
        ], function(results){
          // Wait before updating the row
          setTimeout(function() {
            querySequence(conn.db, [
              'UPDATE ' + escId(table) + ' SET `col` = 15'
            ], function(results){
              // ...
            });
          }, 100);
        });

      });
    });
  },
  // Cases specified in test/fixtures/multipleQueries.js
  multipleQueries: function(test) {
    var tablePrefix = 'multiple_queries_';
    // Milliseconds between each query execution
    var queryWaitTime = 100;
    server.on('ready', function(conn, esc, escId, queries) {
      Object.keys(multipleQueriesData).forEach(function(queryName) {
        var table = tablePrefix + queryName;
        var tableEsc = escId(table);
        var details = multipleQueriesData[queryName];

        var columnDefStr = _.map(details.columns, function(typeStr, name) {
          return escId(name) + ' ' + typeStr;
        }).join(', ');
        var columnList = Object.keys(details.columns);
        var initDataStr = details.initial.map(function(rowData) {
          return '(' + columnList.map(function(column) {
            return esc(rowData[column]);
          }).join(', ') + ')';
        }).join(', ');
        var replaceTable = function(query) {
          return query.replace(/\$table\$/g, tableEsc);
        };
        querySequence(conn.db, [
          'DROP TABLE IF EXISTS ' + tableEsc,
          'CREATE TABLE ' + tableEsc + ' (' + columnDefStr + ')',
          'INSERT INTO ' + tableEsc + ' (' + columnList.map(escId).join(', ') +
            ') VALUES ' + initDataStr,
        ], function(results) {
          var actualDiffs = [];
          var actualDatas = [];
          var curQuery = 0;
          var oldData = [];
          conn.select(replaceTable(details.select), [ {
            table: table,
            database: server.database,
            condition: details.condition
          } ]).on('update', function(diff, rows) {
            actualDiffs.push(diff);
            actualDatas.push(LiveMysql.applyDiff(oldData, diff));

            oldData = rows;

            if(curQuery < details.queries.length) {
              setTimeout(function() {
                querySequence(conn.db,
                  [ replaceTable(details.queries[curQuery++]) ],
                  function(results){ /* do nothing with results */ });
              }, queryWaitTime);
            }

            if(actualDiffs.length === details.expectedDiffs.length) {
              test.deepEqual(actualDiffs, details.expectedDiffs,
                'Diff Mismatch on ' + queryName);

              if(details.expectedDatas) {
                test.deepEqual(actualDatas, details.expectedDatas,
                  'Data Mismatch on ' + queryName);
              }
              test.done();
            }
          });

        });
      });
    });
  },
  checkConditionWhenQueued: function(test) {
    var table = 'check_condition_when_queued';
    server.on('ready', function(conn, esc, escId, queries) {
      // The following line should make no change but it is here for
      //  explicitness
      conn.settings.checkConditionWhenQueued = false;

      querySequence(conn.db, [
        'DROP TABLE IF EXISTS ' + escId(table),
        'CREATE TABLE ' + escId(table) + ' (col INT UNSIGNED)',
        'INSERT INTO ' + escId(table) + ' (col) VALUES (10)',
      ], function(results) {
        var conditionCountUnder1000 = 0;
        var conditionCountOver1000 = 0;
        conn.select('SELECT * FROM ' + escId(table), [ {
          table: table,
          database: server.database,
          condition: function(row, newRow, rowDeleted) {
            if(newRow.col < 1000) {
              // Under 1000, checkConditionWhenQueued is false
              // Will not bother rechecking the condition when query is
              //  queued to be refreshed already
              conditionCountUnder1000++;
            } else {
              // Over 1000, checkConditionWhenQueued is true
              // Condition will be checked with every row that changes
              conditionCountOver1000++;
            }
            return true;
          }
        } ]).on('update', function(diff, rows){
          if(rows.length > 0 && rows[0].col === 2000){
            conn.settings.checkConditionWhenQueued = false;
            test.equal(conditionCountUnder1000, 1);
            test.equal(conditionCountOver1000, 4);
            test.done();
          }
        });

        querySequence(conn.db, [
          'UPDATE ' + escId(table) + ' SET `col` = `col` + 5',
          'UPDATE ' + escId(table) + ' SET `col` = `col` + 5',
          'UPDATE ' + escId(table) + ' SET `col` = `col` + 5',
          'UPDATE ' + escId(table) + ' SET `col` = 1000',
        ], function(results){
          // Should have only had one condition function call at this point
          conn.settings.checkConditionWhenQueued = true;

          querySequence(conn.db, [
            'UPDATE ' + escId(table) + ' SET `col` = `col` + 5',
            'UPDATE ' + escId(table) + ' SET `col` = `col` + 5',
            'UPDATE ' + escId(table) + ' SET `col` = `col` + 5',
            'UPDATE ' + escId(table) + ' SET `col` = 2000',
          ], function(results){
            // Should have seen all of these updates in condition function
          });
        });
      });
    });
  },
  pauseAndResume: function(test) {
    var waitTime = 500;
    var table = 'pause_resume';
    server.on('ready', function(conn, esc, escId, queries) {
      querySequence(conn.db, [
        'DROP TABLE IF EXISTS ' + escId(table),
        'CREATE TABLE ' + escId(table) + ' (col INT UNSIGNED)',
        'INSERT INTO ' + escId(table) + ' (col) VALUES (10)',
      ], function(results) {
        var pauseTime = Date.now();
        conn.select('SELECT * FROM ' + escId(table), [ {
          table: table,
          database: server.database
        } ]).on('update', function(diff, rows){
          if(rows.length > 0 && rows[0].col === 10){
            test.ok(true);
            conn.pause();
            setTimeout(function(){
              conn.resume();
            }, waitTime);
          }else if(rows.length > 0 && rows[0].col === 15){
            // Ensure that waiting occurred
            test.ok(Date.now() - pauseTime > waitTime);
            test.done();
          }
        });

        querySequence(conn.db, [
          'UPDATE ' + escId(table) +
          ' SET `col` = 15'
        ], function(results){
          // ...
        });
      });
    });
  },
  stopAndActive: function(test) {
    // NOTE: Must be last test that uses binlog updates since it calls stop()
    var table = 'stop_active';
    server.on('ready', function(conn, esc, escId, queries) {
      querySequence(conn.db, [
        'DROP TABLE IF EXISTS ' + escId(table),
        'CREATE TABLE ' + escId(table) + ' (col INT UNSIGNED)',
        'INSERT INTO ' + escId(table) + ' (col) VALUES (10)',
      ], function(results){
        var query = 'SELECT * FROM ' + escId(table);
        conn.select(query, [ {
          table: table,
          database: server.database
        } ]).on('update', function(diff, rows) {
          if(rows.length > 0 && rows[0].col === 10) {
            test.ok(true);
          }else if(rows.length > 0 && rows[0].col === 15) {
            test.ok(this.active());
            this.stop();
            // When all instances of query removed, resultsBuffer removed too
            test.equal(typeof conn._queryCache[query], 'undefined');

            test.ok(!this.active());
            conn.db.query('DELETE FROM ' + escId(table));
            setTimeout(function() {
              test.done();
            }, 100);
          }else if(rows.length === 0) {
            throw new Error('Select should have been stopped!');
          }
        });

        querySequence(conn.db, [
          'UPDATE ' + escId(table) +
          ' SET `col` = 15'
        ], function(results){
          // ...
        });
      });
    });
  },
  immediate_disconnection: function(test){
    var errorOccurred = false;
    // Update serverId setting to prevent collision
    settings.serverId++;

    var myTest = new LiveMysql(settings).on('error', function(error){
      errorOccurred = true;
    }).on('ready', function() {
      myTest.end();
      test.equal(errorOccurred, false);
      settings.serverId--;
      test.done();
    });
  },
  error_invalid_connection: function(test){
    var myTest = new LiveMysql({
      host: '127.0.0.1',
      port: 12345,
      user: 'not-working',
      password: 'hahhaha'
    }).on('error', function(error){
      test.equal(error.code, 'ECONNREFUSED');
      test.done();
    });
  },
  error_invalid_connection_callback: function(test){
    var myTest = new LiveMysql({
      host: '127.0.0.1',
      port: 12345,
      user: 'not-working',
      password: 'hahhaha'
    }, function(error){
      test.equal(error.code, 'ECONNREFUSED');
      test.done();
    });
  },
  error_no_db_selected: function(test){
    server.on('ready', function(conn, esc, escId, queries){

      test.throws(function(){
        conn.select('SELECT 1+1', [ { table: 'fake_table' } ]);
      }, /no database selected on trigger/);

      test.throws(function(){
        conn.select('SELECT 1+1');
      }, /triggers array required/);

      test.throws(function(){
        conn.select('SELECT 1+1', []);
      }, /triggers array required/);

      test.done();

    });
  },
  error_invalid_query: function(test){
    var table = 'error_invalid_query';
    server.on('ready', function(conn, esc, escId, queries){
      querySequence(conn.db, [
        'DROP TABLE IF EXISTS ' + escId(table),
        'CREATE TABLE ' + escId(table) + ' (col INT UNSIGNED)',
      ], function(results){
        conn.select('SELECT notcol FROM ' + escId(table), [ {
          table: table,
          database: server.database
        } ]).on('error', function(error){
          test.ok(error.toString().match(/ER_BAD_FIELD_ERROR/));
          test.done();
        });

      });
    });
  }
}
