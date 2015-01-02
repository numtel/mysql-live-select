/* mysql-live-select, MIT License ben@latenightsketches.com
   test/helpers/connector.js - Connect to database */
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var LiveMysql = require('../../');
var querySequence = require('./querySequence');

function Connector(settings){
  var self = this;
  EventEmitter.call(self);
  self.database = settings.database;
  delete settings.database;
  self.conn = new LiveMysql(settings);
  self.ready = false;
  var escId = self.conn.db.escapeId;
  var esc = self.conn.db.escape.bind(self.conn.db);

  querySequence(self.conn.db, [
    'DROP DATABASE IF EXISTS ' + escId(self.database),
    'CREATE DATABASE ' + escId(self.database),
    'USE ' + escId(self.database),
  ], function(results){
    self.ready = true;
    self.emit('ready', self.conn, esc, escId);
  });

  setTimeout(function(){
    self.on('newListener', function(event, listener){
      if(event === 'ready'){
        if(self.ready) listener(self.conn, esc, escId);
      }
    });
  }, 1);

};

util.inherits(Connector, EventEmitter);

module.exports = Connector;
