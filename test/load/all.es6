var fs = require('fs')
var spawn = require('child_process').spawn

var _ = require('lodash')

var cases = fs.readdirSync('test/load/cases').map(filename =>
  filename.substr(0, filename.length - 4))

// Skip this, causes problems
_.pull(cases, 'common.getClient')

// Run each case for 30 mins
const DURATION = 30 * 60 * 1000

function runCase(caseName) {
  return new Promise((resolve, reject) => {
    var child = spawn('node', [
      'test/load/',
      '--conn',
      options.conn,
      '--channel',
      options.channel,
      '--case',
      caseName
    ])

    child.stdout.pipe(process.stdout)
    child.stderr.pipe(process.stderr)

    var timeout = setTimeout(() => {
      child.kill('SIGINT')
    }, DURATION)

    child.on('close', code => {
      clearTimeout(timeout)
      console.log('exited with code', code)
      resolve(code)
    })
  })
}

async function runAll() {
  for(let caseName of cases){
    await runCase(caseName)
  }
}

runAll()
