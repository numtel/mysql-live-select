# pg-notify-trigger

This node.js package/sample application provides notifications on row changes.

1. Run `npm install` to download dependent packages.

2. Configure the database connection and query in `src/index.es6`.

3. Load `join-example.sql` into Postgres. Change the owner user from 'meteor', if needed.

4. Then run `npm run make-start` to build the ES6 files and start the app at index.js.

## PgTriggers Class

### constructor(conn, channel)

Both arguments are required

Argument | Type | Description
---------|------|---------------------------
`conn`     | `object` | Active `any-db` PostgreSQL connection
`channel` | `string` | Unique identifier for this connection. Used as channel for `NOTIFY` commands as well as prefix for triggers, functions, and views.

### createTrigger(table, payloadColumns) Method

Both arguments are required

Argument | Type | Description
---------|------|---------------------------
`table` | `string` | Name of table to place trigger for any row change
`payloadColumns` | `[string]` | Array of column names to include in change event payload

Returns `RowTrigger` instance.

### select(query, triggers) Method

Both arguments are required

Argument | Type | Description
---------|------|---------------------------
`query` | `string` | SQL `SELECT` statement
`triggers` | `object` | Description of when and how to refresh the query results, described below.

> Parameters in the `query` argument must be escaped and inserted into the string. See [node-postgres Issue #440](https://github.com/brianc/node-postgres/issues/440) for more information.
> 
> **TODO** A Javascript function must be implemented in order to safely escape values.

Returns `LiveSelect` instance.

#### Trigger description object

A `LiveSelect` trigger description object defines which tables (and which columns on those tables) to watch for changes as well has whether or not refresh the result set. If refreshing the result set, a selection to replace may be made.

```javascript
{
  // For each table to watch, specify a lambda function with arguments
  // matching the column names you need in order to determine if the query
  // needs to be updated
  table_name_to_watch: function(id, another_column) {
    // Does change row's id column match myId variable?
    return id === myId ? 
      // Yes, refresh result set where column watcher_id matches id
      { watcher_id : id } :
      // No, do not refresh
      false
  }
}
```

### cleanup(callback) method

Perform pre-shutdown cleanup of triggers, functions and any other temporary data.

Argument `callback` requires function which accepts `error, result` arguments.

### Events Emitted

Event Name | Arguments | Description
---------|------|---------------------------
`change:<table_name>` | `payload` | A change notification has arrived for the specific table
`error` | `error` | Unhandled exceptions will be thrown

`PgTriggers` instances allow an unlimited number of event listeners.

## LiveSelect Class

### constructor(parent, query, triggers)

Parent must be `PgTriggers` instance.

Other arguments defined above in the `PgTriggers.select` method definition.

### refresh(condition) Method

The `condition` argument is required.
* Pass `true` to refresh entire result set
* Or, pass an object defining a selection to replace by specifying the column name as the key with the accepted value as the value (e.g. To replace where column `assignment_id` is `7`, `{ assignment_id: 7 }`)

### throttledRefresh(condition) Method

Same as `refresh` method except will not perform operations faster than 1 per second

### Events Emitted

Event Name | Arguments | Description
---------|------|---------------------------
`diff` | `diff` | Array of changes to result set, called before updating data
`update` | `rows` | New result set, called after updating data
`error` | `error` | Unhandled exceptions will be thrown


## RowTrigger Class

### constructor(parent, table, payloadColumns)

Parent must be `PgTriggers` instance.

Other arguments defined above in the `PgTriggers.createTrigger` method definition.

### Events Emitted

Event Name | Arguments | Description
---------|------|---------------------------
`ready` | `results` | Triggers have been installed on the table
`change` | `payload` | A row has changed on this table
`error` | `error` | Unhandled exceptions will be thrown

