/* mysql-live-select, MIT License ben@latenightsketches.com
   test/index.js - Test Suite */
var LiveMysql = require('../');
var settings = require('./settings/mysql');
var querySequence = require('./helpers/querySequence');
var Connector = require('./helpers/connector');
var server = new Connector(settings);

module.exports = {
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
              test.done();
            }, 100);
          }
        })
        .on('added', error)
        .on('changed', error)
        .on('removed', error);

        querySequence(conn.db, [
          'UPDATE ' + escId(table) +
          ' SET `col` = 15'
        ], function(results){
          // ...
        });
      });
    });
  },
  error_no_db_selected: function(test){
    var table = 'error_no_db';
    server.on('ready', function(conn, esc, escId, queries){
      querySequence(conn.db, [
        'DROP TABLE IF EXISTS ' + escId(table),
        'CREATE TABLE ' + escId(table) + ' (col INT UNSIGNED)',
        'INSERT INTO ' + escId(table) + ' (col) VALUES (10)',
      ], function(results){

        conn.select('SELECT * FROM ' + escId(table), [ {
          table: table
        } ]).on('error', function(error){
          test.equal(error.toString(),
            'Error: no database selected on trigger');
          test.done();
        });

        querySequence(conn.db, [
          'UPDATE ' + escId(table) +
          ' SET `col` = 15'
        ], function(results){
          // ...
        });
      });
    });
  }
}
