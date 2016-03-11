# mysql-live-select [![Build Status](https://travis-ci.org/numtel/mysql-live-select.svg?branch=master)](https://travis-ci.org/numtel/mysql-live-select)

NPM Package to provide events when a MySQL select statement result set changes.

Built using the [`zongji` Binlog Tailer](https://github.com/nevill/zongji) and [`node-mysql`](https://github.com/felixge/node-mysql) projects.

* [Example Application using Express, SockJS and React](https://github.com/numtel/reactive-mysql-example)
* [Meteor package for reactive MySQL](https://github.com/numtel/meteor-mysql)
* [NPM Package for Sails.js connection adapter integration](https://github.com/numtel/sails-mysql-live-select)
* [Analogous package for PostgreSQL, `pg-live-select`](https://github.com/numtel/pg-live-select)

This package has been tested to work in MySQL 5.1, 5.5, 5.6, and 5.7. Expected support is all MySQL server version >= 5.1.15.

## Installation

* Add the package to your project:
  ```bash
  $ npm install mysql-live-select
  ```

* Enable MySQL binlog in `my.cnf`, restart MySQL server after making the changes.

  ```
  # Must be unique integer from 1-2^32
  server-id        = 1
  # Row format required for ZongJi
  binlog_format    = row
  # Directory must exist. This path works for Linux. Other OS may require
  #   different path.
  log_bin          = /var/log/mysql/mysql-bin.log

  binlog_do_db     = employees   # Optional, limit which databases to log
  expire_logs_days = 10          # Optional, purge old logs
  max_binlog_size  = 100M        # Optional, limit log size
  ```
* Create an account, then grant replication privileges:

  ```sql
  GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'user'@'localhost'
  ```

## LiveMysql Constructor

The `LiveMysql` constructor makes 3 connections to your MySQL database:

* Connection for executing `SELECT` queries (exposed `node-mysql` instance as `db` property)
* Replication slave connection
* `information_schema` connection for column information

#### Arguments

Argument | Type | Description
---------|------|---------------------------
`settings` | `object` | An object defining the settings. In addition to the [`node-mysql` connection settings](https://github.com/felixge/node-mysql#connection-options), the additional settings below are available.
`callback` | `function` | **Deprecated:** callback on connection success/failure. Accepts one argument, `error`. See information below about events emitted.

#### Additional Settings

Setting | Type | Description
--------|------|------------------------------
`serverId`  | `integer` | [Unique number (1 - 2<sup>32</sup>)](http://dev.mysql.com/doc/refman/5.0/en/replication-options.html#option_mysqld_server-id) to identify this replication slave instance. Must be specified if running more than one instance.<br>**Default:** `1`
`minInterval` | `integer` | Pass a number of milliseconds to use as the minimum between result set updates. Omit to refresh results on every update. May be changed at runtime.
`checkConditionWhenQueued` | `boolean` | Set to `true` to call the condition function of a query on every binlog row change event. By default (when undefined or `false`), the condition function will not be called again when a query is already queued to be refreshed. Enabling this can be useful if external caching of row changes.

#### Events Emitted

Use `.on(...)` to handle the following event types.

Event Name | Arguments | Description
-----------|-----------|---------------
`error`    | `Error` | An error has occurred.
`ready`    | *None*  | The database connection is ready.

#### Quick Start

```javascript
// Example:
var liveConnection = new LiveMysql(settings);
var table = 'players';
var id = 11;

liveConnection.select(function(esc, escId){
  return (
    'select * from ' + escId(table) +
    'where `id`=' + esc(id)
  );
}, [ {
  table: table,
  condition: function(row, newRow){
    // Only refresh the results when the row matching the specified id is
    // changed.
    return row.id === id
      // On UPDATE queries, newRow must be checked as well
      || (newRow && newRow.id === id);
  }
} ]).on('update', function(diff, data){
  // diff contains an object describing the difference since the previous update
  // data contains an array of rows of the new result set
  console.log(data);
});
```
See [`example.js`](example.js) for full source...


### LiveMysql.prototype.select(query, triggers)

Argument | Type | Description
---------|------|----------------------------------
`query`  | `string` or `function` | `SELECT` SQL statement. See note below about passing function.
`triggers` | `[object]` | Array of objects defining which row changes to update result set

Returns `LiveMysqlSelect` object

#### Function as `query`

A function may be passed as the `query` argument that accepts two arguments.

* The first argument, `esc` is a function that escapes values in the query.
* The second argument, `escId` is a function that escapes identifiers in the query.

#### Trigger options

Name | Type | Description
-----|------|------------------------------
`table` | `string` | Name of table (required)
`database` | `string` | Name of database (optional)<br>**Default:** `database` setting specified on connection
`condition` | `function` | Evaluate row values (optional)

#### Condition Function

A condition function accepts up to three arguments:

Argument Name | Description
--------------|-----------------------------
`row`         | Table row data
`newRow`      | New row data (only available on `UPDATE` queries, `null` for others)
`rowDeleted`  | Extra argument for aid in external caching: `true` on `DELETE`  queries, `false` on `INSERT`  queries, `null` on `UPDATE`  queries.

Return `true` when the row data meets the condition to update the result set.

### LiveMysql.prototype.pause()

Temporarily skip processing of updates from the binary log.

### LiveMysql.prototype.resume()

Begin processing updates after `pause()`. All active live select instances will be refreshed upon resume.

### LiveMysql.prototype.end()

Close connections and stop checking for updates.

### LiveMysql.applyDiff(data, diff)

Exposed statically on the LiveMysql object is a function for applying a `diff` given in an `update` event to an array of rows given in the `data` argument.

## LiveMysqlSelect object

Each call to the `select()` method on a LiveMysql object, returns a `LiveMysqlSelect` object with the following methods:

Method Name | Arguments | Description
------------|-----------|-----------------------
`on`, `addListener` | `event`, `handler` | Add an event handler to the result set. See the following section for a list of the available event names.
`update`    | `callback` | Update the result set. Callback function accepts `error, rows` arguments. Events will be emitted.
`stop`      | *None* | Stop receiving updates
`active`    | *None* | Return `true` if ready to recieve updates, `false` if `stop()` method has been called.

As well as all of the other methods available on [`EventEmitter`](http://nodejs.org/api/events.html)...

### Available Events

Event Name | Arguments | Description
-----------|-----------|---------------------------
`update` | `diff`, `data` | First argument contains an object describing the difference since the previous `update` event with `added`, `removed`, `moved`, and `copied` rows. Second argument contains complete result set array.
`error` | `error` | Unhandled errors will be thrown

## Running Tests

Tests must be run with a properly configured MySQL server. Configure test settings in `test/settings/mysql.js`.

Execute [Nodeunit](https://github.com/caolan/nodeunit) using the `npm test` command.

## License

MIT
