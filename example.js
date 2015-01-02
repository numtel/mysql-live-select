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
  condition: function(row, newRow){ return row.id === id; }
} ]).on('update', function(data){
  console.log(data);
});

