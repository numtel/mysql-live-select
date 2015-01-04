/* mysql-live-select, MIT License ben@latenightsketches.com
   lib/LiveMysql.js - Main Class */
var ZongJi = require('../zongji'); // Not yet available on NPM
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
  // Cache query results for any new, duplicate SELECT statements
  self._resultsBuffer = {};

  db.connect();

  zongji.on('binlog', function(event) {
    if(event.getEventName() === 'tablemap') return;
    if(self._select.length === 0) return;
    
    // Cache query results within this update event
    var eventResults = {};

    function _nextSelect(index){
      var select;
      if(index < self._select.length){
        select = self._select[index];
        if(select.matchRowEvent(event)){
          if(select.query in eventResults){
            select._setRows(eventResults[select.query]);
            _nextSelect(index + 1);
          }else{
            select.update(function(error, rows){
              if(error === undefined){
                eventResults[select.query] = rows;
              }
              _nextSelect(index + 1);
            });
          }
        }else{
          _nextSelect(index + 1);
        }
      }
    }

    _nextSelect(0);
    
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
