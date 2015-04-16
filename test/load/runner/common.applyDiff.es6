var _ = require('lodash')

var sampleRows = require('../../fixtures/' + settings.sampleFixture)
console.time('Generating_sample_cases')
var generations = _.range(500).map(index => generateDiffArgs())
console.timeEnd('Generating_sample_cases')
var curGen = 0

var common = require('../../../src/common')

module.exports = async function() {
  var gen = generations[(curGen++) % generations.length]
  var newData = common.applyDiff(gen.data, gen.diff)

  if(newData.length !== sampleRows.length) {
    console.log('LENGTH_MISMATCH', index, newData.length, sampleRows.length)
  }

  newData = null
  gen = null
}

/**
 * For a given collection of data, generate a random sample from its items
 * @param Integer length Optionally, specify a length of the return collection
 *                        If ommitted, the length will be random
 */
function getRandomSample(data, length) {
  var sample = []
  var sampleIndexes = []

  if(length === undefined) {
    // Use random sample size if not specified
    length = Math.ceil(Math.random() * data.length)
  }

  while(sample.length < length) {
    var randomIndex = Math.floor(Math.random() * data.length)
    var randomRow = _.clone(data[randomIndex])

    // Skip duplicates
    if(sampleIndexes.indexOf(randomRow._index) === -1) {
      sample.push(randomRow)
      sampleIndexes.push(randomRow._index)
    }
  }

  length = null
  randomIndex = null
  randomRow = null
  sampleIndexes = null

  return sample
}

/**
 * Using the sampleRows, create a result set (data) and a diff that when
 *  combined, will result in a full array of sampleRows, albeit in a random
 *  order.
 */
function generateDiffArgs() {
  var data = getRandomSample(sampleRows).map((row, index) => {
    row._index = index + 1
    return row
  })

  var removed = getRandomSample(data)
    .map(row => _.pick(row, '_index'))

  var removedHashes = _.pluck(removed, '_index')

  var moved = getRandomSample(data)
    .map(row => _.pick(row, '_index'))
    .filter(row => removedHashes.indexOf(row._index) === -1)
  // Shuffle indices
  moved = moved.map((row, index) => {
    let newArrayIndex = index === 0 ? moved.length - 1 : index - 1
    return {
      old_index: row._index,
      new_index: moved[newArrayIndex]._index
    }
  })

  // As a test, copy all removed rows to a different index
  // Also, it helps keep the indexes lined up
  var copied = removed.slice()
  // Shuffle indices
  copied = copied.map((row, index) => {
    let newArrayIndex = index === 0 ? copied.length - 1 : index - 1
    return {
      orig_index: row._index,
      new_index: copied[newArrayIndex]._index
    }
  })

  var dataHashes = _.pluck(data, '_hash')

  // Place the balance of sampleRows not used in data into addedRows
  var added = sampleRows
    .filter(row => dataHashes.indexOf(row._hash) === -1)
    .map((row, index) => {
      var newRow = _.clone(row)
      newRow._index = data.length + index + 1
      return newRow
    })

  var diff = {
    removed: removed.length !== 0 ? removed : null,
    moved: moved.length !== 0 ? moved : null,
    copied: copied.length !== 0 ? copied : null,
    added: added.length !== 0 ? added : null
  }

  dataHashes    = null
  removedHashes = null
  removed       = null
  moved         = null
  copied        = null
  added         = null

  return { data, diff }
}
