var _ = require('lodash')

var common = require('../../../src/common')

module.exports = async function() {
  var conn = await common.getClient(options.conn)

  var details = await common.getQueryDetails(conn.client, settings.query)

  if(_.xor(details.tablesUsed, settings.expectedTables).length !== 0) {
    console.error('UNEXPECTED RESULT', details.tablesUsed)
  }

  if(details.isUpdatable !== settings.expectUpdatable) {
    console.error('MISMATCHED IS_UPDATABLE')
  }

  conn.done()
}

