module.exports = {
  host        : 'localhost',
  user        : 'root',
  password    : 'numtel',
  database    : 'live_select_test',
  serverId    : 347,
  minInterval : 200
};

if(process.env.TRAVIS){
  // Port to use is passed as variable
  module.exports.port = process.env.TEST_MYSQL_PORT;
}

