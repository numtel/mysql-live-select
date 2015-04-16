var common = require('../../../src/common')

module.exports = async function() {
  var table = 'assignments'
  var channel = 'test_channel'

  var conn = await common.getClient(options.conn)

  await common.createTableTrigger(conn.client, table, channel)

  await common.dropTableTrigger(conn.client, table, channel)

  conn.done()
}

