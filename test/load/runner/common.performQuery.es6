var _ = require('lodash')

var common = require('../../../src/common')

module.exports = async function () {
  var conn = await common.getClient(options.conn)

  await common.performQuery(conn.client, settings.query, settings.params)

  conn.done()
}
