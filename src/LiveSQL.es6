var EventEmitter = require('events').EventEmitter
var _            = require('lodash')
var murmurHash   = require('murmurhash-js').murmur3
var sqlParser    = require('sql-parser')

var common       = require('./common')
var RateCounter  = require('./RateCounter')
var SelectHandle = require('./SelectHandle')

/*
 * Global flag to disable simple query optimization
 * Simple queries are those which select from only one table without any
 *  aggregate functions, OFFSET, or GROUP BY used
 * When enabled, these queries will keep the result set current without
 *  repeatedly executing the query on each change
 * TODO Notification payload pagination at 8000 bytes
 */
const ENABLE_SIMPLE_QUERIES = false

/*
 * Duration (ms) to wait to check for new updates when no updates are
 *  available in current frame
 */
const STAGNANT_TIMEOUT = 100

class LiveSQL extends EventEmitter {
	constructor(connStr, channel) {
		this.connStr           = connStr
		this.channel           = channel
		this.notifyHandle      = null
		this.waitingToUpdate   = []
		this.selectBuffer      = {}
		this.tablesUsed        = {}
		this.queryDetailsCache = {}
		this.refreshRate       = new RateCounter
		// XXX Extra stats for debugging load test
		this.refreshCount      = 0
		this.notifyCount       = 0

		this.ready = this.init()
	}

	async init() {
		this.notifyHandle = await common.getClient(this.connStr)

		await common.performQuery(this.notifyHandle.client,
			`LISTEN "${this.channel}"`)

		this.notifyHandle.client.on('notification', info => {
			if(info.channel === this.channel) {
				// XXX this.notifyCount is only used for debugging the load test
				this.notifyCount++

				try {
					// See common.createTableTrigger() for payload definition
					var payload = JSON.parse(info.payload)
				} catch(error) {
					return this.emit('error',
						new Error('INVALID_NOTIFICATION ' + info.payload))
				}

				if(payload.table in this.tablesUsed) {
					for(let queryHash of this.tablesUsed[payload.table]) {
						let queryBuffer = this.selectBuffer[queryHash]
						if((queryBuffer.triggers
								// Check for true response from manual trigger
								&& payload.table in queryBuffer.triggers
								&& (payload.op === 'UPDATE'
									// Rows changed in an UPDATE operation must check old and new
									? queryBuffer.triggers[payload.table](payload.new_data[0])
										|| queryBuffer.triggers[payload.table](payload.old_data[0])
									// Rows changed in INSERT/DELETE operations only check once
									: queryBuffer.triggers[payload.table](payload.data[0])))
							|| (queryBuffer.triggers
								// No manual trigger for this table
								&& !(payload.table in  queryBuffer.triggers))
							|| !queryBuffer.triggers) {

							if(queryBuffer.parsed !== null) {
								queryBuffer.notifications.push(payload)
							}

							// TODO simple queries need not wait!
							this.waitingToUpdate.push(queryHash)
						}
					}
				}
			}
		})

		// Initialize neverending loop to refresh active result sets
		var performNextUpdate = () => {
			if(this.waitingToUpdate.length !== 0) {
				let queriesToUpdate =
					_.uniq(this.waitingToUpdate.splice(0, this.waitingToUpdate.length))

				// XXX this.refreshCount is only used for debugging the load test
				this.refreshCount += queriesToUpdate.length

				this.refreshRate.inc(queriesToUpdate.length)

				Promise.all(queriesToUpdate.map(queryHash =>
					this._updateQuery(queryHash))).then(performNextUpdate)
			}
			else {
				// No queries to update, wait for set duration
				setTimeout(performNextUpdate, STAGNANT_TIMEOUT)
			}
		}.bind(this)
		performNextUpdate()
	}

	select(query, params, triggers) {
		// Allow omission of params argument
		if(typeof params === 'object' && !(params instanceof Array)) {
			triggers = params
			params = []
		}
		else if(typeof params === 'undefined') {
			params = []
		}

		if(typeof query !== 'string')
			throw new Error('QUERY_STRING_MISSING')
		if(!(params instanceof Array))
			throw new Error('PARAMS_ARRAY_MISMATCH')

		let queryHash = murmurHash(JSON.stringify([ query, params ]))
		let handle = new SelectHandle(this, queryHash)

		// Perform initialization asynchronously
		this._initSelect(query, params, triggers, queryHash, handle)
			.catch(reason => this.emit('error', reason))

		return handle
	}

	async _initSelect(query, params, triggers, queryHash, handle) {
		if(queryHash in this.selectBuffer) {
			let queryBuffer = this.selectBuffer[queryHash]

			queryBuffer.handlers.push(handle)

			// Give a chance for event listener to be added
			await common.delay()

			// Initial results from cache
			handle.emit('update',
				{ removed: null, moved: null, copied: null, added: queryBuffer.data },
				queryBuffer.data)
		}
		else {
			// Initialize result set cache
			let newBuffer = this.selectBuffer[queryHash] = {
				query,
				params,
				triggers,
				data          : [],
				handlers      : [ handle ],
				// Queries that have parsed property are simple and may be updated
				//  without re-running the query
				parsed        : null,
				notifications : []
			}

			let pgHandle = await common.getClient(this.connStr)
			let queryDetails
			if(query in this.queryDetailsCache) {
				queryDetails = this.queryDetailsCache[query]
			}
			else {
				queryDetails = await common.getQueryDetails(pgHandle.client, query)
				this.queryDetailsCache[query] = queryDetails
			}

			if(ENABLE_SIMPLE_QUERIES && queryDetails.isUpdatable) {
				// Query parser does not support tab characters
				let cleanQuery = query.replace(/\t/g, ' ')
				// TODO move sqlParser to separate process to prevent event loop block?
				//  or use external native postgres parser?
				try {
					newBuffer.parsed = sqlParser.parse(cleanQuery)
				} catch(error) {
					// Not a serious error, fallback to using full refreshing
				}

				// OFFSET and GROUP BY not supported with simple queries
				if(newBuffer.parsed
					&& ((newBuffer.parsed.limit && newBuffer.parsed.limit.offset)
						|| newBuffer.parsed.group)) {
					newBuffer.parsed = null
				}
				// Ensure that table used has primary key
				else if(queryDetails.primaryKeys.length === 0) {
					newBuffer.parsed = null
				}
				// Ensure that query selects primary key column
				else if(!(newBuffer.parsed.fields.length === 1
						&& newBuffer.parsed.fields[0].constructor.name === 'Star')
					&& newBuffer.parsed.fields.filter(item =>
						queryDetails.primaryKeys.indexOf(item.field.value) !== -1).length === 0) {
					newBuffer.parsed = null
				}
			}

			for(let table of queryDetails.tablesUsed) {
				if(!(table in this.tablesUsed)) {
					this.tablesUsed[table] = [ queryHash ]
					await common.createTableTrigger(pgHandle.client, table, this.channel)
				}
				else if(this.tablesUsed[table].indexOf(queryHash) === -1) {
					this.tablesUsed[table].push(queryHash)
				}
			}

			pgHandle.done()

			// Retrieve initial results
			this.waitingToUpdate.push(queryHash)
		}
	}

	async _updateQuery(queryHash) {
		let pgHandle = await common.getClient(this.connStr)

		let queryBuffer = this.selectBuffer[queryHash]
		let update
		if(queryBuffer.parsed !== null
			// Notifications array will be empty for initial results
			&& queryBuffer.notifications.length !== 0) {

			update = await common.getDiffFromSupplied(
				pgHandle.client,
				queryBuffer.data,
				queryBuffer.notifications.splice(0, queryBuffer.notifications.length),
				queryBuffer.query,
				queryBuffer.parsed,
				queryBuffer.params
			)
		}
		else{
			update = await common.getResultSetDiff(
				pgHandle.client,
				queryBuffer.data,
				queryBuffer.query,
				queryBuffer.params
			)
		}

		pgHandle.done()

		if(update !== null) {
			queryBuffer.data = update.data

			for(let updateHandler of queryBuffer.handlers) {
				updateHandler.emit('update',
					filterHashProperties(update.diff), filterHashProperties(update.data))
			}
		}
	}

	async cleanup() {
		this.notifyHandle.done()

		let pgHandle = await common.getClient(this.connStr)

		for(let table of Object.keys(this.tablesUsed)) {
			await common.dropTableTrigger(pgHandle.client, table, this.channel)
		}

		pgHandle.done()
	}
}

module.exports = LiveSQL
// Expose SelectHandle class so it may be modified by application
module.exports.SelectHandle = SelectHandle

function filterHashProperties(diff) {
	if(diff instanceof Array) {
		return diff.map(event => {
			return _.omit(event, '_hash')
		})
	}
	// Otherwise, diff is object with arrays for keys
	_.forOwn(diff, (rows, key) => {
		diff[key] = filterHashProperties(rows)
	})
	return diff
}
