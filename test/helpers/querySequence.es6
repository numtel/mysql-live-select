var common = require('../../src/common')

async function querySequence(queries) {
  var connection = await common.getClient(process.env.CONN)
  var client = connection.client

  var results = []

  if(queries.length === 0) return results

  for(let query of queries){
//    console.log('runnin query', query)
    results.push(await performQuery(client, query))
  }

  if(connection) {
    connection.done()
  }

  return results
}

module.exports = querySequence

function performQuery(client, query) {
  return new Promise((resolve, reject) => {
    var queryComplete = (error, rows, fields) => {
      if(error) reject(error)
      else resolve(rows)
    }

    if(query instanceof Array) {
      client.query(query[0], query[1], queryComplete)
    }
    else {
      client.query(query, queryComplete)
    }
  })
}
