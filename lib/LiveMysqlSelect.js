/* mysql-live-select, MIT License ben@latenightsketches.com
   lib/LiveMysqlSelect.js - Select Result Set Class */
var EventEmitter = require('events').EventEmitter;
var util = require('util');

function LiveMysqlSelect(queryCache, triggers, base){
  if(!queryCache)
    throw new Error('queryCache required');
  if(!(triggers instanceof Array))
    throw new Error('triggers array required');
  if(typeof base !== 'object')
    throw new Error('base LiveMysql instance required');

  var self = this;
  EventEmitter.call(self);
  self.triggers = triggers;
  self.base = base;
  self.data = [];
  self.queryCache = queryCache;
  queryCache.selects.push(self);
  
  if(queryCache.initialized){
    var refLastUpdate = queryCache.lastUpdate;
    
    // Trigger events for existing data
    setTimeout(function() {
      if(queryCache.lastUpdate !== refLastUpdate){
        // Query cache has been updated since this select object was created;
        // our data would've been updated already.
        return;
      }
      
      self.emit('update', queryCache.data);
      
      if(queryCache.data.length !== 0 && !self.base.settings.skipDiff){
        var diff = queryCache.data.map(function(row, index) { return [ 'added', row, index ]; });
        diff.forEach(function(evt){
          self.emit.apply(self, evt);
          // New row added to end
          self.data[evt[2]] = evt[1];
        });
        // Output all difference events in a single event
        self.emit('diff', diff);
      }
    }, 50);
  }else{
    queryCache.invalidate();
  }
}

util.inherits(LiveMysqlSelect, EventEmitter);

LiveMysqlSelect.prototype.matchRowEvent = function(event){
  var self = this;
  var tableMap = event.tableMap[event.tableId];
  var eventName = event.getEventName();
  var trigger, row;
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
            return trigger.condition.call(self, row.before, row.after);
          }else{
            // writerows or deleterows
            return trigger.condition.call(self, row);
          }
        }
      }
    }
  }
  return false;
};

LiveMysqlSelect.prototype.stop = function(){
  var self = this;
  return self.base._removeSelect(self);
};

LiveMysqlSelect.prototype.active = function(){
  var self = this;
  return self.base._select.indexOf(self) !== -1;
};

module.exports = LiveMysqlSelect;
