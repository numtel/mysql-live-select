/* mysql-live-select, MIT License ben@latenightsketches.com
   test/index.js - Test Suite */
var LiveMysql = require('../');
var settings = require('./settings/mysql');
var querySequence = require('./helpers/querySequence');
var Connector = require('./helpers/connector');
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
        'INSERT INTO ' + escId(table) + ' (col) VALUES (10)',
      ], function(results){
        queries.splice(0, queries.length);

        var query = 'SELECT * FROM ' + escId(table);
        var triggers = [ {
          database: server.database,
          table: table
        } ];

        conn.select(query, triggers).on('update', function(data){
          // After initial update
          if(data.length > 0 && data[0].col === 10){
            // Second select instance to check resultsBuffer
            conn.select(query, triggers).on('update', function(data){
              if(data.length > 0 && data[0].col === 15){
                // [1] Test in LiveMysqlSelect created later,
                // Ensure only First select, update, second select occurred
                // i.e. No duplicate selects, resultsBuffer working
                test.equal(queries.length, 3);
                conn.db.query('DELETE FROM ' + escId(table));
              }
            }).on('added', function(row, index){
              test.equal(index, 0);
              test.equal(row.col, 10);
            }).on('changed', function(row, newRow, index){
              test.equal(index, 0);
              test.equal(row.col, 10);
              test.equal(newRow.col, 15);
            }).on('removed', function(row, index){
              test.equal(index, 0);
              test.equal(row.col, 15);
            }).on('diff', function(diff){
              // Only one row will change at once
              test.equal(diff.length, 1);
              // Index will always be 0, the first item
              test.equal(diff[0][diff[0].length - 1], 0);
              switch(diff[0][0]){
                case 'added':
                  test.equal(diff[0][1].col, 10);
                  break;
                case 'changed':
                  test.equal(diff[0][1].col, 10);
                  test.equal(diff[0][2].col, 15);
                  break;
                case 'added':
                  test.equal(diff[0][1].col, 15);
                  break;
              }
            });

            querySequence(conn.db, [
              'UPDATE ' + escId(table) +
              ' SET `col` = 15'
            ], function(results){
              // ...
            });
          }
        }).on('added', function(row, index){
          test.equal(index, 0);
          test.equal(row.col, 10);
        }).on('changed', function(row, newRow, index){
          test.equal(index, 0);
          test.equal(row.col, 10);
          test.equal(newRow.col, 15);
        }).on('removed', function(row, index){
          test.equal(index, 0);
          test.equal(row.col, 15);
          test.done();
        });

      });
    });
  },
  skipDiff: function(test){
    var table = 'skip_diff';
    server.on('ready', function(conn, esc, escId, queries){
      querySequence(conn.db, [
        'DROP TABLE IF EXISTS ' + escId(table),
        'CREATE TABLE ' + escId(table) + ' (col INT UNSIGNED)',
        'INSERT INTO ' + escId(table) + ' (col) VALUES (10)',
      ], function(results){
        var error = function(){
          throw new Error('diff events should not be called');
        };

        conn.settings.skipDiff = true;
        conn.select('SELECT * FROM ' + escId(table), [ {
          table: table,
          database: server.database
        } ]).on('update', function(rows){
          if(rows.length > 0 && rows[0].col === 10){
            test.ok(true);
          }else if(rows.length > 0 && rows[0].col === 15){
            conn.db.query('DELETE FROM ' + escId(table));
          }else if(rows.length === 0){
            // Give time, just in case the `removed` event comes in
            setTimeout(function(){
              conn.settings.skipDiff = false;
              test.done();
            }, 100);
          }
        })
        .on('added', error)
        .on('changed', error)
        .on('removed', error)
        .on('diff', error);

        querySequence(conn.db, [
          'UPDATE ' + escId(table) +
          ' SET `col` = 15'
        ], function(results){
          // ...
        });
      });
    });
  },
  pauseAndResume: function(test){
    var waitTime = 500;
    var table = 'pause_resume';
    server.on('ready', function(conn, esc, escId, queries){
      querySequence(conn.db, [
        'DROP TABLE IF EXISTS ' + escId(table),
        'CREATE TABLE ' + escId(table) + ' (col INT UNSIGNED)',
        'INSERT INTO ' + escId(table) + ' (col) VALUES (10)',
      ], function(results){
        var pauseTime = Date.now();
        conn.select('SELECT * FROM ' + escId(table), [ {
          table: table,
          database: server.database
        } ]).on('update', function(rows){
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
  stopAndActive: function(test){
    // Must be last test that uses binlog updates since it calls stop()
    var table = 'stop_active';
    server.on('ready', function(conn, esc, escId, queries){
      querySequence(conn.db, [
        'DROP TABLE IF EXISTS ' + escId(table),
        'CREATE TABLE ' + escId(table) + ' (col INT UNSIGNED)',
        'INSERT INTO ' + escId(table) + ' (col) VALUES (10)',
      ], function(results){
        conn.select('SELECT * FROM ' + escId(table), [ {
          table: table,
          database: server.database
        } ]).on('update', function(rows){
          if(rows.length > 0 && rows[0].col === 10){
            test.ok(true);
          }else if(rows.length > 0 && rows[0].col === 15){
            test.ok(this.active());
            this.stop();
            test.ok(!this.active());
            conn.db.query('DELETE FROM ' + escId(table));
            setTimeout(function(){
              test.done();
            }, 100);
          }
        }).on('removed', function(row, index){
          throw new Error('should not be called');
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
  error_invalid_connection: function(test){
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
