/* mysql-live-select, MIT License ben@latenightsketches.com
   lib/LiveMysql.js - Main Class */
var ZongJi = require('zongji');
var mysql = require('mysql');
var LiveMysqlSelect = require('./LiveMysqlSelect');

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
      if(select.matchRowEvent(event)){
        select.update();
      }
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

LiveMysql.prototype.end = function(){
  var self = this;
  self.zongji.stop();
  self.db.destroy();
};


module.exports = LiveMysql;
