/* mysql-live-select, MIT License ben@latenightsketches.com, wj32.64@gmail.com
   lib/QueryCache.js - Query Results Cache Class

Many LiveMysqlSelect objects can share the same query cache if they have the
same query string.

*/

var _ = require('lodash');
var md5 = require('md5');

var differ = require('./differ');

function QueryCache(query, base) {
  if(!query)
    throw new Error('query required');

  var self = this;
  self.base = base;
  self.query = query;
  self.needUpdate = false;
  self.updating = false;
  self.lastUpdate = 0;
  self.data = [];
  self.oldHashes = [];
  self.selects = [];
  self.initialized = false;
  self.updateTimeout = null;
}

QueryCache.prototype.setData = function(rows) {
  var self = this;
  self.data = rows;

  for(var i = 0; i < self.selects.length; i++) {
    self.selects[i].data = self.data;
  }
};

QueryCache.prototype._emitOnSelects = function(/* arguments */) {
  var self = this;
  for(var i = 0; i < self.selects.length; i++){
    var select = self.selects[i];
    select.emit.apply(select, arguments);
  }
};

QueryCache.prototype.matchRowEvent = function(event) {
  var self = this;
  for(var i = 0; i < self.selects.length; i++) {
    if(self.selects[i].matchRowEvent(event)) return true;
  }
  return false;
};

QueryCache.prototype.invalidate = function() {
  var self = this;

  function update() {
    // Refuse to send more than one query out at a time. Note that the code
    // below always sets needUpdate to false; when the current query finishes
    // running we will check needUpdate again and re-run the query if necessary.
    if(self.updating) {
      self.needUpdate = true;
      return;
    }

    self.updating = true;
    self.needUpdate = false;

    // Perform the update
    self.base.db.query(self.query, function(error, rows) {
      self.updating = false;

      if(error) return self._emitOnSelects('error', error);

      if(rows.length === 0 && self.initialized === false) {
        // If the result set initializes to 0 rows, it still needs to output an
        //  update event.
        self._emitOnSelects('update',
          { removed: null, moved: null, copied: null, added: [] },
          []
        );
      } else {
        // Perform deep clone of new data to be modified for the differ
        var rowsForDiff = _.cloneDeep(rows);

        var newHashes = rows.map(function(row, index) {
          var hash = md5(JSON.stringify(row));

          // Provide the differ with the necessary details
          rowsForDiff[index]._hash = hash;
          rowsForDiff[index]._index = index + 1;

          return hash;
        });

        var diff =
          filterHashProperties(differ.generate(self.oldHashes, rowsForDiff));

        if(diff !== null) {
          self._emitOnSelects('update', diff, rows);

          // Now that event has been emitted, the new becomes the old
          self.oldHashes = newHashes;
        }
      }

      self.setData(rows);

      self.initialized = true;
      self.lastUpdate = Date.now();

      if(self.needUpdate === true) update();
    });
  }

  if(typeof self.base.settings.minInterval !== 'number') {
    update();
  } else if(self.lastUpdate + self.base.settings.minInterval < Date.now()) {
    update();
  } else { // Before minInterval
    if(self.updateTimeout === null){
      self.updateTimeout = setTimeout(function(){
        self.updateTimeout = null;
        update();
      }, self.lastUpdate + self.base.settings.minInterval - Date.now());
    }
  }
};

module.exports = QueryCache;

function filterHashProperties(diff, alsoIndex) {
  if(diff instanceof Array) {
    var omitKeys = [ '_hash' ];
    if(alsoIndex) omitKeys.push('_index');

    return diff.map(function(event) {
      return _.omit(event, omitKeys)
    });
  }
  // Otherwise, diff is object with arrays for keys
  _.forOwn(diff, function(rows, key) {
    diff[key] = filterHashProperties(rows, alsoIndex)
  });
  return diff;
}

