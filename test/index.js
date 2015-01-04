/* mysql-live-select, MIT License ben@latenightsketches.com
   test/index.js - Test Suite */
var LiveMysql = require('../');
var settings = require('./settings/mysql');
var querySequence = require('./helpers/querySequence');
var Connector = require('./helpers/connector');
var server = new Connector(settings);

module.exports = {
  basic: function(test){
    var table = 'simple';
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
          if(data[0].col === 10){
            // Second select instance to check resultsBuffer
            conn.select(query, triggers).on('update', function(data){
              if(data[0].col === 15){
                // Test in LiveMysqlSelect created later,
                // Ensure only First select, update, second select occurred
                // i.e. No duplicate selects, resultsBuffer working
                test.equal(queries.length, 3);
                test.done();
              }
            });

            querySequence(conn.db, [
              'UPDATE ' + escId(table) +
              ' SET `col` = 15'
            ], function(results){
              // ...
            });
          }
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
