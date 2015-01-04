# mysql-live-select

NPM Package to provide events when a MySQL select statement result set changes.

Built using the [`zongji` Binlog Tailer](https://github.com/nevill/zongji) and [`node-mysql`](https://github.com/felixge/node-mysql) projects.

## Installation

* Add the package to your project:
  ```bash
  $ npm install mysql-live-select
  ```

* Enable MySQL binlog in `my.cnf`, restart MySQL server after making the changes.

  ```
  # binlog config
  server-id        = 1
  binlog_format    = row
  log_bin          = /usr/local/var/log/mysql/mysql-bin.log
  binlog_do_db     = employees   # optional
  expire_logs_days = 10          # optional
  max_binlog_size  = 100M        # optional
  ```
* Create an account with replication privileges:

  ```sql
  GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'localhost'
  ```

## LiveMysql Constructor

The `LiveMysql` constructor makes 3 connections to your MySQL database:

* Connection for executing `SELECT` queries
* Replication slave connection
* `information_schema` connection for column information

One argument, an object defining the settings. In addition the [`node-mysql` connection settings](#...), the following settings are available:

Setting | Type | Description
--------|------|------------------------------
`serverId`  | `integer` | [Unique number (1 - 2<sup>32</sup>)](http://dev.mysql.com/doc/refman/5.0/en/replication-options.html#option_mysqld_server-id) to identify this replication slave instance. Must be specified if running more than one instance.<br>**Default:** `1`
`minInterval` | `integer` | Pass a number of milliseconds to use as the minimum between result set updates. Omit to refresh results on every update. May be changed at runtime.
`skipDiff` | `boolean` | If `true`, the `added`, `changed`, and `removed` events will not be emitted. May be changed at runtime.<br>**Default:** `false`

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
  condition: function(row, newRow){ return row.id === id; }
} ]).on('update', function(data){
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

A condition function accepts one or two arguments:

Argument Name | Description
--------------|-----------------------------
`row`         | Table row data
`newRow`      | New row data (only available on `UPDATE` queries)

Return `true` when the row data meets the condition to update the result set.

## LiveMysqlSelect object

Each call to the `select()` method on a LiveMysql object, returns a `LiveMysqlSelect` object with the following methods:

Method Name | Arguments | Description
------------|-----------|-----------------------
`on`, `addListener` | `event`, `handler` | Add an event handler to the result set. See the following section for a list of the available event names.
`update`    | `callback` | Update the result set. Callback function accepts `error, rows` arguments. Events will be emitted.

As well as all of the other methods available on [`EventEmitter`](http://nodejs.org/api/events.html)...

### Available Events

Event Name | Arguments | Description
-----------|-----------|---------------------------
`update` | `rows` | Single argument contains complete result set array. Called before `added`, `changed`, and `removed` events.
`added` | `row`, `index` | Row added to result set at index
`changed` | `row`, `newRow`, `index` | Row contents mutated at index
`removed` | `row`, `index` | Row removed at index
`error` | `error` | Unhandled errors will be thrown

## Running Tests

Tests must be run with a properly configured MySQL server. Configure test settings in `test/settings.mysql.js`.

Execute `nodeunit` using the `npm test` command.

## License

MIT
