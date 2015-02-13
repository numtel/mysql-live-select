/**
 * Pass filenames from load-test.sh as arguments
 * e.g. node load-test-parse.js oot.log
 */
var fs = require('fs');
var path = require('path');

process.argv.slice(2).forEach(function(filename) {
  console.log(filename);
  var results = fs.readFileSync(filename, "utf8").split('index.js').map(
    function(log){
      try{
        return {
          scoresCount: parseInt(log.match(/Scores.length:  (\d+)/)[1]),
          duration: parseInt(log.match(/\((\d+)ms\)/)[1])
        }
      }catch(err){
        // Skip it, probably an error
      }
    }
  ).filter(function(details){
    return details != null
  });

  results.forEach(function(result){
    console.log(result.scoresCount);
  });
  results.forEach(function(result){
    console.log(result.duration);
  });
  console.log(JSON.stringify(results));
});
