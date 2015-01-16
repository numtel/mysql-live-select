module.exports = {
  host        : 'localhost',
  user        : 'root',
  password    : 'numtel',
  database    : 'live_select_test',
  serverId    : 347,
  minInterval : 200
};

if(process.env.TRAVIS){
  // Travis CI database root user does not have a password
  module.exports.password = '';
  // Port to use is passed as variable
  module.exports.port = process.env.TEST_MYSQL_PORT;
}

