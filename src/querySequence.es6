var begin = require('any-db-transaction');

// Execute a sequence of queries on a database connection
// @param {object} connection - Connection, Connected, has query() method
// @param {boolean} debug - Print queries as they execute (optional)
// @param {[string]} queries - Queries to execute, in order
// @param {function} callback - Call when complete (error, results)
module.exports = function(connection, debug, queries, callback){
  if(debug instanceof Array){
    callback = queries;
    queries = debug;
    debug = false;
  }
  var results = [];
  var transaction = begin(connection);

  transaction.on('error', callback);

  var sequence = queries.map(function(query, index, initQueries){
    var tmpCallback = function(err, rows, fields) {
      if(err) callback(err);

      results.push(rows);

      if(index < sequence.length - 1){
        sequence[index + 1]();
      }else{
        transaction.commit();
        callback(null, results);
      }
    };

    return function(){
      debug && console.log('Query Sequence', index, query);

      if(query instanceof Array) {
        transaction.query(query[0], query[1], tmpCallback);
      }
      else {
        transaction.query(query, tmpCallback);
      }
    }
  });

  sequence[0]();
};
