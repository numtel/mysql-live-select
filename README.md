# pg-live-query

This package exposes the `PgTriggers` class in order to provide realtime result sets for PostgreSQL `SELECT` statements.

## Implements

### PgTriggers Class

The `PgTriggers` constructor requires 2 arguments:

Constructor Argument | Type | Description
---------|------|---------------------------
`connectionString` | `string` | ex. `postgres://user:pass@host/db`
`channel` | `string` | Unique identifier for this connection. Used as channel for `NOTIFY` commands as well as prefix for triggers, functions, and views. Must follow SQL naming rules: may only contain `a-z`, `A-Z`, `0-9`, and `_` and not begin with a number.

A single persistent client is used to listen for notifications. Result set refreshes obtain additional clients from the pool on-demand.

Each instance offers the following methods:

Method Name | Returns | Description
-------------|--------|---------------------
`select(query, params)` | `LiveSelect` instance | Instantiate a live updating `SELECT` statement for a given query. Pass query string as first argument, `query`. Optionally, pass an array to the second arguments, `params`, with values for placeholders (e.g. `$1`).
`cleanup(callback)` | `Promise` | Perform pre-shutdown cleanup of triggers, functions and any other temporary data. Optional argument `callback` requires function which accepts `error, result` arguments.
`getClient(callback)` | *None* | Obtain a Postgres client from the pool. Required callback accepts `error`, `client`, and `done` arguments. From the [node-postgres wiki](https://github.com/brianc/node-postgres/wiki/pg): *If you do not call `done()` the client will never be returned to the pool and you will leak clients. This is mega-bad so always call `done()`.*

The following events are emitted:

Event Name | Arguments | Description
---------|------|---------------------------
`change:<table_name>` | *None*  | A change notification has arrived for the specific table
`error` | `error` | Unhandled exceptions will be thrown

`PgTriggers` instances allow an unlimited number of event listeners.

### LiveSelect Class

Instantiate a `LiveSelect` using the `PgTriggers.select()` method. Each instance offers the following methods:

Method Name | Description
-----------|-----------------------------
`refresh()` | Update the result set immediately.
`throttledRefresh()` | Same as `refresh` method except will not perform operations more frequently than 1 per second.
`stop()` | Stop receiving updates on this instance.

And emits the following events:

Event Name | Arguments | Description
---------|------|---------------------------
`update` | `diff` | Array containing description of changes
`ready` | *None* | All triggers have been installed, initial results to follow
`error` | `error` | Unhandled exceptions will be thrown

## Simple Example

1. Run `npm install` to download dependent packages.

2. Load `join-example.sql` into Postgres. Change the owner user from 'meteor', if needed.

3. Configure the database connection string and query in `src/index2.es6`.

4. Then run `npm run make && node lib/index2.js` to build the ES6 files and start the app at index2.js.

## Run Test Suite

1. Configure connection string in `package.json`.

2. Run the suite with `npm run make && npm test`.
