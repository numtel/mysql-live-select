# pg-live-query

This package exposes the `LiveSQL` class in order to provide realtime result sets for PostgreSQL `SELECT` statements.

## Implements

### LiveSQL Class

The `LiveSQL` constructor requires 2 arguments:

Constructor Argument | Type | Description
---------|------|---------------------------
`connectionString` | `string` | ex. `postgres://user:pass@host/db`
`channel` | `string` | Unique identifier for this connection. Used as channel for `NOTIFY` commands as well as prefix for triggers, functions, and views. Must follow SQL naming rules: may only contain `a-z`, `A-Z`, `0-9`, and `_` and not begin with a number.

A single persistent client is used to listen for notifications. Result set refreshes obtain additional clients from the pool on-demand.

Each instance offers the following asynchronous methods (both return Promises):

Method | Returns | Description
-------|---------|-----------------
`select(query, params, onUpdate)` | `{ stop() }` handle `Object` | Call `onUpdate` with new data on initialization and each change. `query` only accepts string. Optional `params` argument accepts array.
`cleanup()` | *Undefined* | Drop all table triggers and close all connections.

## Simple Example

1. Run `npm install` to download dependent packages.

2. Load `join-example.sql` into Postgres. Change the owner user from 'meteor', if needed.

3. Configure the database connection string and query in `src/index2.es6`.

4. Then run `npm run make && node lib/index2.js` to build the ES6 files and start the app at index2.js.

## Perfoming Tests

### Regression Suite

1. Configure connection string in `package.json`.

2. Run the suite with `npm run make && npm test`.

### Performance / Memory Usage

The load test application may be ran using `node test/load/`.

Check your configuration options using `node test/load/ --help`.

The default case performs a general end-to-end test on the entire package.

Other cases are available: (Use the `--case=<case name>` command line option)

Case name | Description
----------|----------------
`static` | Default case, perform steady load of `INSERT` and `UPDATE` operations
`spikes` | Perform spiking loads operations
`baseline` | No operation, control case
`common.getClient` | Obtain and release client from connection pool
`common.tableTriggers` | Create and drop triggers/functions on a table
`common.performQuery` | Obtain client, perform simple query (1+1) then release client
`common.performQuery.scoresLoad` | Perform query that matches the `static` case
`common.performQuery.scoresLoadDiff` | Perform query to get difference on `static` case
`common.getResultSetDiff` | Get difference of current result set from last known
`common.applyDiff.20` | Apply random differences to a small result set (20 rows)
`common.applyDiff.800` | Apply random differences to a large result set (800 rows)

## License

LGPL V2.1
