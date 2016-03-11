/* mysql-live-select, MIT License ben@latenightsketches.com
   lib/LiveMysql.js - Main Class */
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var ZongJi = require('zongji');
var mysql = require('mysql');

// Maximum duration to wait for Zongji to initialize before timeout error (ms)
var ZONGJI_INIT_TIMEOUT = 1500;

var LiveMysqlSelect = require('./LiveMysqlSelect');
var QueryCache = require('./QueryCache');

function LiveMysql(settings, callback){
  var self = this;
  EventEmitter.call(this);

  if(callback) {
    console.log(
      '\nDEPRECATED: Callback on LiveMysql constructor deprecated! \n' +
      '  Use .on() \'error\' and \'ready\' instead.');
    self.on('error', callback);
    self.on('ready', callback);
  }

  var db = mysql.createConnection(settings);

  self.settings = settings;
  self.zongji = null;
  self.db = db;
  self._select = [];
  self._queryCache = {};
  self._schemaCache = {};

  self.zongjiSettings = {
    serverId: settings.serverId,
    startAtEnd: true,
    includeEvents: [ 'tablemap', 'writerows', 'updaterows', 'deleterows' ],
    includeSchema: self._schemaCache
  };

  self.db.connect(function(error){
    if(error) return self.emit('error', error);

    var zongji = self.zongji = new ZongJi(self.settings);

    zongji.on('error', function(error) {
      self.emit('error', error);
    });

    zongji.on('binlog', function(event) {
      if(event.getEventName() === 'tablemap') return;

      Object.keys(self._queryCache).forEach(function(query) {
        var curCache = self._queryCache[query];
        if((self.settings.checkConditionWhenQueued
            || curCache.updateTimeout === null)
          && curCache.matchRowEvent(event)){
          curCache.invalidate();
        }
      });
    })

    // Wait for Zongji to be ready before executing callback
    var zongjiInitTime = Date.now();
    var zongjiReady = function() {
      if(zongji.ready === true) {
        // Call the callback if it exists and do not keep waiting
        self.emit('ready');
      } else {
        // Wait for Zongji to be ready
        if(Date.now() - zongjiInitTime > ZONGJI_INIT_TIMEOUT) {
          // Zongji initialization has exceeded timeout, callback error
          self.emit('error', new Error('ZONGJI_INIT_TIMEOUT_OCCURED'));
        } else {
          setTimeout(zongjiReady, 40);
        }
      }
    };
    zongji.start(self.zongjiSettings);
    zongjiReady();
  });
}

util.inherits(LiveMysql, EventEmitter);

LiveMysql.prototype.select = function(query, triggers){
  var self = this;

  if(!(triggers instanceof Array) ||
     triggers.length === 0)
    throw new Error('triggers array required');

  // Update schema included in ZongJi events
  var includeSchema = self._schemaCache;
  for(var i = 0; i < triggers.length; i++){
    var triggerDatabase = triggers[i].database || self.settings.database;
    if(triggerDatabase === undefined){
      throw new Error('no database selected on trigger');
    }
    if(!(triggerDatabase in includeSchema)){
      includeSchema[triggerDatabase] = [ triggers[i].table ];
    }else if(includeSchema[triggerDatabase].indexOf(triggers[i].table) === -1){
      includeSchema[triggerDatabase].push(triggers[i].table);
    }
  }
  
  query = self._escapeQueryFun(query);
  
  var queryCache;
  if(self._queryCache.hasOwnProperty(query)){
    queryCache = self._queryCache[query];
  }else{
    queryCache = new QueryCache(query, this);
    self._queryCache[query] = queryCache;
  }

  var newSelect = new LiveMysqlSelect(queryCache, triggers, this);
  self._select.push(newSelect);
  return newSelect;
};

LiveMysql.prototype._escapeQueryFun = function(query){
  var self = this;
  if(typeof query === 'function'){
    var escId = self.db.escapeId;
    var esc = self.db.escape.bind(self.db);
    return query(esc, escId);
  }
  return query;
};

LiveMysql.prototype._removeSelect = function(select){
  var self = this;
  var index = self._select.indexOf(select);
  if(index !== -1){
    // Remove the select object from our list
    self._select.splice(index, 1);
    
    var queryCache = select.queryCache;
    var queryCacheIndex = queryCache.selects.indexOf(select);
    if(queryCacheIndex !== -1){
      // Remove the select object from the query cache's list and remove the
      // query cache if no select objects are using it.
      queryCache.selects.splice(queryCacheIndex, 1);
      if(queryCache.selects.length === 0){
        delete self._queryCache[queryCache.query];
      }
    }
    
    return true;
  }else{
    return false;
  }
}

LiveMysql.prototype.pause = function(){
  var self = this;
  self.zongjiSettings.includeSchema = {};
  self.zongji.set(self.zongjiSettings);
};

LiveMysql.prototype.resume = function(){
  var self = this;
  self.zongjiSettings.includeSchema = self._schemaCache;
  self.zongji.set(self.zongjiSettings);

  // Update all select statements
  Object.keys(self._queryCache).forEach(function(query) {
    self._queryCache[query].invalidate();
  });
};

LiveMysql.prototype.end = function(){
  var self = this;
  self.zongji.stop();
  self.db.destroy();
};

// Expose child constructor for prototype enhancements
LiveMysql.LiveMysqlSelect = LiveMysqlSelect;

// Expose diff apply function statically
LiveMysql.applyDiff = require('./differ').apply;

module.exports = LiveMysql;
