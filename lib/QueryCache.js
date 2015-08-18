 /* mysql-live-select, MIT License wj32.64@gmail.com
   lib/QueryCache.js - Query Results Cache Class */

// Many LiveMysqlSelect objects can share the same query cache if they have the
// same query string.

function QueryCache(query, base){
  if(!query)
    throw new Error('query required');

  var self = this;
  self.base = base;
  self.query = query;
  self.needUpdate = false;
  self.updating = false;
  self.lastUpdate = 0;
  self.data = [];
  self.selects = [];
  self.initialized = false;
}

QueryCache.prototype._setDataOnSelects = function(){
  var self = this;
  for(var i = 0; i < self.selects.length; i++){
    self.selects[i].data = self.data;
  }
};

QueryCache.prototype._emitOnSelects = function(name, arg){
  var self = this;
  for(var i = 0; i < self.selects.length; i++){
    self.selects[i].emit(name, arg);
  }
};

QueryCache.prototype._emitApplyOnSelects = function(evt){
  var self = this;
  for(var i = 0; i < self.selects.length; i++){
    var select = self.selects[i];
    select.emit.apply(select, evt);
  }
};

QueryCache.prototype._setRows = function(rows){
  var self = this;
  var diff = [];

  // Determine what changes before updating cache in order to
  // be able to skip all event emissions if no change
  // TODO update this algorithm to use less data
  rows.forEach(function(row, index){
    if(self.data.length - 1 < index){
      diff.push([ 'added', row, index ]);
    }else if(JSON.stringify(self.data[index]) !== JSON.stringify(row)){
      diff.push([ 'changed', self.data[index], row, index ]);
    }
  });

  if(self.data.length > rows.length){
    for(var i = self.data.length - 1; i >= rows.length; i--){
      diff.push([ 'removed', self.data[i], i ]);
    }
  }

  if(diff.length !== 0){
    self._emitOnSelects('update', rows);
    // Make sure the relevant select objects have the right data array.
    self._setDataOnSelects();

    diff.forEach(function(evt){
      if(!self.base.settings.skipDiff){
        self._emitApplyOnSelects(evt);
      }
      switch(evt[0]){
        case 'added':
          // New row added to end
          self.data[evt[2]] = evt[1];
          break;
        case 'changed':
          // Update row data reference
          self.data[evt[3]] = evt[2];
          break;
        case 'removed':
          // Remove extra rows off the end
          self.data.splice(evt[2], 1);
          break;
      }
    });
    if(!self.base.settings.skipDiff){
      // Output all difference events in a single event
      self._emitOnSelects('diff', diff);
    }
  }else if(self.initialized === false){
    // If the result set initializes to 0 rows, it still needs to output an
    //  update event.
    self._emitOnSelects('update', rows);
  }

  self.initialized = true;
  self.lastUpdate = Date.now();
};

QueryCache.prototype.matchRowEvent = function(event){
  var self = this;
  for(var i = 0; i < self.selects.length; i++){
    var select = self.selects[i];
    if (select.matchRowEvent(event)){
      return true;
    }
  }
  return false;
};

QueryCache.prototype.canSkipRowEvent = function(){
  var self = this;
  return self.base.settings.canSkipRowEvents && self._updateTimeout !== undefined;
};

QueryCache.prototype.invalidate = function(){
  var self = this;

  function update(){
    // Refuse to send more than one query out at a time. Note that the code
    // below always sets needUpdate to true; when the current query finishes
    // running we will check needUpdate again and re-run the query if necessary.
    if(self.updating){
      self.needUpdate = true;
      return;
    }
    self.updating = true;
    self.needUpdate = false;
    self.base.db.query(self.query, function(error, rows){
      self.updating = false;
      if(error){
        self._emitOnSelects('error', error);
      }else{
        self._setRows(rows);
        if(self.needUpdate){
          update();
        }
      }
    });
  }

  if(self.base.settings.minInterval === undefined){
    update();
  }else if(self.lastUpdate + self.base.settings.minInterval < Date.now()){
    update();
  }else{ // Before minInterval
    if(!self._updateTimeout){
      self._updateTimeout = setTimeout(function(){
        delete self._updateTimeout;
        update();
      }, self.lastUpdate + self.base.settings.minInterval - Date.now());
    }
  }
};

module.exports = QueryCache;
