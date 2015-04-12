# pg-live-query

NPM Package to provide events when a PostgreSQL `SELECT` statement result set changes.

Built using [node-postgres NPM package](https://github.com/brianc/node-postgres).

**Postgres 9.3+ is required.**

## Installation

```
npm install pg-live-query
```

## LivePG Class

The `LivePG` constuctor requires 2 arguments:

Constructor Argument | Type | Description
---------|------|---------------------------
`connectionString` | `string` | ex. `postgres://user:pass@host/db`
`channel` | `string` | Unique identifier for this connection. Used as channel for `NOTIFY` commands as well as prefix for triggers, functions, and views. Must follow SQL naming rules: may only contain `a-z`, `A-Z`, `0-9`, and `_` and not begin with a number.

A single persistent client is used to listen for notifications. Result set refreshes obtain additional clients from the pool on-demand.

```javascript
// Instantiate LivePG class
var LivePG = require('pg-live-query');

var liveDb = new LivePG('postgres://user:pass@host/db', 'myapp');

// Create a live query
var highScores = liveDb
	.select('SELECT * FROM scores WHERE score > 10')
	.on('update', function(diff, data) {
		// diff: object containing differences since last update
		// data: array of full result set
	});

// Stop query updates
highScores.stop();

// When exiting the application, remove all installed triggers
liveDb.cleanup().then(function() {
	// Database is now cleaned
});
```

See working examples in [`src/index2.es6`](src/index2.es6), [`src/index2.manual.es6`](src/index2.manual.es6), and [`src/index2.simple.es6`](src/index2.simple.es6).

The `LivePG` class inherits from `EventEmitter`, providing `error` events.

### LivePG.prototype.select(query, [params], [triggers])

Argument | Type | Description
---------|------|--------------------
`query` | `String` | `SELECT` SQL statement
`params` | `Array` | Optionally, pass an array of parameters to interpolate into the query safely. Paramaters in the `query` are denoted using `$<number>`, e.g. `$1` corresponds to `params[0]`. If omitted, `triggers` may occupy the second argument.
`triggers` | `Object` | Optionally, specify an object defining invalidation lamdba functions for specific tables. If omitted, the query results will be refreshed on any change to the query's dependent tables.

Returns `SelectHandle` object.

#### Trigger object definitions

The `triggers` argument object contains table names as the object's keys and result set data invalidation functions as values. Each function returns a boolean value determining whether the query results should be refreshed on account of the row that has changed.

* For `INSERT` operations, the new row is passed as the argument.
* For `UPDATE` operations, the function is called twice: once with the old row as the argument and once with the new row as the argument. If either returns true, the query results are updated.
* For `DELETE` operations, the old row is passed as the argument.

```javascript
// Simple live query with custom trigger
liveDb.select('SELECT * FROM scores WHERE score > $1', [ 10 ], {
	'scores': function(row) {
		return row.score > 10
	}
})
```

#### SelectHandle class

The `LivePG.prototype.select()` method returns an instance of the `SelectHandle` class that contains a `stop()` method for terminating updates to a live query.

The `SelectHandle` class inherits from `EventEmitter`, providing an `update` event on each result set change with two arguments: `diff` and `data`. `diff` contains a description of which rows have been `added`, `moved`, `removed`, and `copied`. `data` contains an array of the full result set.

## Getting started with the examples

1. Run `npm install` to download dependent packages.

2. Load `join-example.sql` into Postgres. Change the owner user from `meteor`, if needed.

3. Configure the database connection string and query in `src/index2.es6`.

4. Then run `npm run make && node lib/index2.js` to build the ES6 files and start the app at index2.js.

## Perfoming Tests

Regression tests are performed using the `npm test` command.

Please see the [Performing Tests wiki page](https://github.coma/nothingisdead/pg-live-query/wiki/Performing-Tests) for information about the load tests.

## License

MIT
