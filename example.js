/* mysql-live-select, MIT License ben@latenightsketches.com
   example.js - Use mysql < example.sql to get started */
var LiveMysql = require('./');

var settings = {
  host        : 'localhost',
  user        : 'root',
  password    : 'numtel',
  database    : 'leaderboard',
  serverId    : 34,
  minInterval : 200
};

var liveConnection = new LiveMysql(settings);
var table = 'players';
var id = 11;

liveConnection.select(function(esc, escId){
  return (
    'select * from ' + escId(table) +
    'where `id`=' + esc(id)
  );
}, [ {
  table: table,
  condition: function(row, newRow){
    // Only refresh the results when the row matching the specified id is
    // changed.
    return row.id === id
      // On UPDATE queries, newRow must be checked as well
      || (newRow && newRow.id === id);
  }
} ]).on('update', function(diff, data){
  // diff contains an object describing the difference since the previous update
  // data contains an array of rows of the new result set
  console.log(data);
});

