var extend       = require('extend')
var pg           = require('pg')
var randomString = require('random-strings')

module.exports = exports = {

	/**
	 * Obtain a node-postgres client from the connection pool
	 * @param  String  connectionString "postgres://user:pass@host/database"
	 * @return Promise { client, done() } Call done() to return client to pool!
	 */
	getClient(connectionString) {
		return new Promise((resolve, reject) => {
			pg.connect(connectionString, (error, client, done) => {
				if(error) reject(error)
				else resolve({ client, done })
			})
		})
	},

	/**
	 * Perform a query
	 * @param  Object client node-postgres client
	 * @param  String query  SQL statement
	 * @param  Array  params Optional, values to substitute into query
	 *                       (params[0] => '$1'...)
	 * @return Promise Array Result set
	 */
	performQuery(client, query, params=[]) {
		return new Promise((resolve, reject) => {
			client.query(query, params, (error, result) => {
				if(error) reject(error)
				else resolve(result)
			})
		})
	},

	/**
	 * Query information_schema to determine tables used by query
	 * @param  Object client node-postgres client
	 * @param  String query  SQL statement, params not used
	 * @return Promise Array Table names
	 */
	async getQueryTables(client, query) {
		var nullifiedQuery = query.replace(/\$\d+/g, 'NULL')
		var viewName = `tmp_view_${randomString.alphaLower(10)}`

		await exports.performQuery(client,
			`CREATE OR REPLACE TEMP VIEW ${viewName} AS (${nullifiedQuery})`)

		var tablesResult = await exports.performQuery(client,
			`SELECT DISTINCT vc.table_name
				FROM information_schema.view_column_usage vc
				WHERE view_name = $1`, [ viewName ])

		await exports.performQuery(client, `DROP VIEW ${viewName}`)

		return tablesResult.rows.map(row => row.table_name)
	},

	/**
	 * Create a trigger to send NOTIFY on any change with payload of table name
	 * @param  Object client  node-postgres client
	 * @param  String table   Name of table to install trigger
	 * @param  String channel NOTIFY channel
	 * @return Promise true   Successful
	 */
	async createTableTrigger(client, table, channel) {
		var triggerName = `${channel}_${table}`

		await exports.performQuery(client,
			`CREATE OR REPLACE FUNCTION ${triggerName}() RETURNS trigger AS $$
				BEGIN
					NOTIFY "${channel}", '${table}';
					RETURN NULL;
				END;
			$$ LANGUAGE plpgsql`)

		await exports.performQuery(client,
			`DROP TRIGGER IF EXISTS "${triggerName}"
				ON "${table}"`)

		await exports.performQuery(client,
			`CREATE TRIGGER "${triggerName}"
				AFTER INSERT OR UPDATE OR DELETE ON "${table}"
				EXECUTE PROCEDURE ${triggerName}()`)

		return true
	},

	/**
	 * Drop matching function and trigger for a table
	 * @param  Object client  node-postgres client
	 * @param  String table   Name of table to remove trigger
	 * @param  String channel NOTIFY channel
	 * @return Promise true   Successful
	 */
	async dropTableTrigger(client, table, channel) {
		var triggerName = `${channel}_${table}`

		await exports.performQuery(client,
			`DROP TRIGGER IF EXISTS ${triggerName} ON ${table}`)

		await exports.performQuery(client,
			`DROP FUNCTION IF EXISTS ${triggerName}()`)

		return true
	},

	/**
	 * Perform SELECT query, obtaining difference in result set
	 * @param  Object  client      node-postgres client
	 * @param  Array   currentData Last known result set for this query/params
	 * @param  String  query       SQL SELECT statement
	 * @param  Array   params      Optionally, pass an array of parameters
	 * @return Promise Object      Enumeration of differences
	 */
	async getResultSetDiff(client, currentData, query, params) {
		var oldHashes = currentData.map(row => row._hash)

		var result = await exports.performQuery(client, `
			WITH
				res AS (${query}),
				data AS (
					SELECT
						MD5(CAST(ROW_TO_JSON(res.*) AS TEXT)) AS _hash,
						ROW_NUMBER() OVER () AS _index,
						res.*
					FROM res),
				data2 AS (
					SELECT
						1 AS _added,
						data.*
					FROM data
					WHERE _hash NOT IN ('${oldHashes.join("','")}'))
			SELECT
				data2.*,
				data._hash AS _hash
			FROM data
			LEFT JOIN data2
				ON (data._index = data2._index)`, params)

		var curHashes = result.rows.map(row => row._hash)
		var newHashes = curHashes.filter(hash => oldHashes.indexOf(hash) === -1)

		// Need copy of curHashes so duplicates can be checked off
		var curHashes2 = curHashes.slice()
		var addedRows = result.rows
			.filter(row => row._added === 1)
			.map(row => {
				// Prepare row meta-data
				row._index = curHashes2.indexOf(row._hash) + 1
				delete row._added

				// Clear this hash so that duplicate hashes can move forward
				curHashes2[row._index - 1] = undefined

				return row
			})
		curHashes2 = null

		var movedHashes = curHashes.map((hash, newIndex) => {
			let oldIndex = oldHashes.indexOf(hash)

			if(oldIndex !== -1 &&
					oldIndex !== newIndex &&
					curHashes[oldIndex] !== hash) {
				return {
					old_index: oldIndex + 1,
					new_index: newIndex + 1,
					_hash: hash
				}
			}
		}).filter(moved => moved !== undefined)

		var removedHashes = oldHashes
			.map((_hash, index) => { return { _hash, _index: index + 1 } })
			.filter(removed =>
				curHashes[removed._index - 1] !== removed._hash &&
				movedHashes.filter(moved =>
					moved.new_index === removed._index).length === 0)

		// Add rows that have already existing hash but in new places
		var copiedHashes = curHashes.map((hash, index) => {
			var oldHashIndex = oldHashes.indexOf(hash)
			if(oldHashIndex !== -1 &&
					oldHashes[index] !== hash &&
					movedHashes.filter(moved =>
						moved.new_index - 1 === index).length === 0 &&
					addedRows.filter(added =>
						added._index - 1 === index).length === 0){
				return {
					new_index: index + 1,
					orig_index: oldHashIndex + 1
				}
			}
		}).filter(copied => copied !== undefined)

		var diff = {
			removed: removedHashes.length !== 0 ? removedHashes : null,
			moved: movedHashes.length !== 0 ? movedHashes: null,
			copied: copiedHashes.length !== 0 ? copiedHashes: null,
			added: addedRows.length !== 0 ? addedRows : null
		}

		removedHashes = null
		movedHashes = null
		copiedHashes = null
		addedRows = null

		if(diff.added === null &&
				diff.moved === null &&
				diff.copied === null &&
				diff.removed === null) return null

		return diff
	},

	/**
	 * Apply a diff to a result set
	 * @param  Array  data Last known full result set
	 * @param  Object diff Output from getResultSetDiff()
	 * @return Array       New result set
	 */
	applyDiff(data, diff) {
		var newResults = data.slice()

		diff.removed !== null && diff.removed
			.forEach(removed => newResults[removed._index - 1] = undefined)

		// Deallocate first to ensure no overwrites
		diff.moved !== null && diff.moved.forEach(moved => {
			newResults[moved.old_index - 1] = undefined
		});

		diff.copied !== null && diff.copied.forEach(copied => {
			var copyRow = extend({}, data[copied.orig_index - 1])
			copyRow._index = copied.new_index
			newResults[copied.new_index - 1] = copyRow
		});

		diff.moved !== null && diff.moved.forEach(moved => {
			var movingRow = data[moved.old_index - 1]
			movingRow._index = moved.new_index
			newResults[moved.new_index - 1] = movingRow
		});

		diff.added !== null && diff.added
			.forEach(added => newResults[added._index - 1] = added)

		return newResults.filter(row => row !== undefined)
	},

}

