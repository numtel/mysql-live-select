var ZongJi = require('zongji');
var mysql = require('mysql');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

function LiveMysql(settings){
  var self = this;
  var zongji = new ZongJi(settings);
  var db = mysql.createConnection(settings);

  self.settings = settings;
  self.zongji = zongji;
  self.db = db;
  self._select = [];

  db.connect();

  zongji.on('binlog', function(event) {
    if(event.getEventName() === 'tablemap') return;

    self._select.forEach(function(select){
      if(select.matchRowEvent(event))
        select.update();
    });
  });

  zongji.start({
    serverId: settings.serverId,
    startAtEnd: true,
    includeEvents: [ 'tablemap', 'writerows', 'updaterows', 'deleterows' ]
  });
}

LiveMysql.prototype.select = function(query, triggers){
  var self = this;
  var newSelect = new LiveMysqlSelect(query, triggers, this);
  self._select.push(newSelect);
  return newSelect;
};

function LiveMysqlSelect(query, triggers, base){
  if(!query)
    throw new Error('query required');
  if(!(triggers instanceof Array))
    throw new Error('triggers array required');
  if(!(base instanceof LiveMysql))
    throw new Error('base LiveMysql instance required');

  var self = this;
  EventEmitter.call(self);
  self.query = self._escapeQueryFun(query);
  self.triggers = triggers;
  self.base = base;
  self.lastUpdate = 0;

  self.update();
}

util.inherits(LiveMysqlSelect, EventEmitter);
// util.inherits(LiveMysqlSelect, Array);


LiveMysqlSelect.prototype._escapeQueryFun: function(query){
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

LiveMysqlSelect.prototype.update = function(){
  var self = this;
  if(self.base.settings.minInterval === undefined ||
     self.lastUpdate + self.base.settings.minInterval < Date.now()){
    self.base.db.query(self.query, function(error, rows){
      if(error){
        self.emit('error', error);
      }else{
        self.lastUpdate = Date.now();
        self.emit('update', rows);
      }
    });
  }
};

module.exports = LiveMysql;
