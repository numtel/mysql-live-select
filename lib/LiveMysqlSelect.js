/* mysql-live-select, MIT License ben@latenightsketches.com
   lib/LiveMysqlSelect.js - Select Result Set Class */
var EventEmitter = require('events').EventEmitter;
var util = require('util');

function LiveMysqlSelect(query, triggers, base){
  if(!query)
    throw new Error('query required');
  if(!(triggers instanceof Array))
    throw new Error('triggers array required');
  if(typeof base !== 'object')
    throw new Error('base LiveMysql instance required');

  var self = this;
  EventEmitter.call(self);
  self.triggers = triggers;
  self.base = base;
  self.lastUpdate = 0;
  self.query = self._escapeQueryFun(query);
  self.data = [];
  self.initialized = false;

  if(self.query in base._resultsBuffer){
    setTimeout(function(){
      self._setRows(base._resultsBuffer[self.query]);
    }, 1);
  }else{
    self.update();
  }
}

util.inherits(LiveMysqlSelect, EventEmitter);

LiveMysqlSelect.prototype._escapeQueryFun = function(query){
  var self = this;
  if(typeof query === 'function'){
    var escId = self.base.db.escapeId;
    var esc = self.base.db.escape.bind(self.base.db);
    return query(esc, escId);
  }
  return query;
};

LiveMysqlSelect.prototype.matchRowEvent = function(event){
  var self = this;
  var tableMap = event.tableMap[event.tableId];
  var eventName = event.getEventName();
  var trigger, row, rowDeleted;
  for(var i = 0; i < self.triggers.length; i++){
    trigger = self.triggers[i];
    triggerDatabase = trigger.database ||
      self.base.settings.database;

    if(triggerDatabase === tableMap.parentSchema &&
       trigger.table === tableMap.tableName){
      if(trigger.condition === undefined){
        return true;
      }else{
        for(var r = 0; r < event.rows.length; r++){
          row = event.rows[r];
          if(eventName === 'updaterows'){
            return trigger.condition.call(self, row.before, row.after, null);
          }else{
            // writerows or deleterows
            rowDeleted = eventName === 'deleterows';
            return trigger.condition.call(self, row, null, rowDeleted);
          }
        }
      }
    }
  }
  return false;
};

LiveMysqlSelect.prototype._setRows = function(rows){
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
    self.emit('update', rows);

    diff.forEach(function(evt){
      if(!self.base.settings.skipDiff){
        self.emit.apply(self, evt);
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
      self.emit('diff', diff);
    }
  }else if(self.initialized === false){
    // If the result set initializes to 0 rows, it still needs to output an
    //  update event.
    self.emit('update', rows);
  }

  self.initialized = true;

  self.lastUpdate = Date.now();
};

LiveMysqlSelect.prototype.update = function(callback){
  var self = this;

  function _update(){
    self.base.db.query(self.query, function(error, rows){
      if(error){
        self.emit('error', error);
        callback && callback.call(self, error);
      }else{
        self.base._resultsBuffer[self.query] = rows;
        self._setRows(rows);
        callback && callback.call(self, undefined, rows);
      }
    });
  }

  if(self.base.settings.minInterval === undefined){
    _update();
  }else if(self.lastUpdate + self.base.settings.minInterval < Date.now()){
    _update();
  }else{ // Before minInterval
    if(!self._updateTimeout){
      self._updateTimeout = setTimeout(function(){
        delete self._updateTimeout;
        _update();
      }, self.lastUpdate + self.base.settings.minInterval - Date.now());
    }
  }
};

LiveMysqlSelect.prototype.stop = function(){
  var self = this;
  
  var index = self.base._select.indexOf(self);
  if(index !== -1){
    self.base._select.splice(index, 1);

    // If no other instance of the same query string, remove the resultsBuffer
    var sameCount = self.base._select.filter(function(select) {
      return select.query === self.query;
    }).length;

    if(sameCount === 0) {
      delete self.base._resultsBuffer[self.query];
    }

    return true;
  }else{
    return false;
  }
};

LiveMysqlSelect.prototype.active = function(){
  var self = this;
  return self.base._select.indexOf(self) !== -1;
};

module.exports = LiveMysqlSelect;
