var LiveMysql = require('./');

var settings = {
  host        : 'localhost',
  user        : 'root',
  password    : 'numtel',
  database    : 'leaderboard',
  serverId    : 34,
  minInterval : 200
  // debug: true
};

var liveConnection = new LiveMysql(settings);
// var result = liveConnection.select(
//   'select * from players',
//   [
//     { table: 'players' }
//   ]
// );
// result.on('update', function(event){
//   // do something to update the client
//   console.log(event);
// });

liveConnection.select('select score from players where id=11', [
  {
    table: 'players',
    condition: function(row, newRow){ return row.id === 11; }
  }
]).on('update', function(event){
  console.log(event);
});
