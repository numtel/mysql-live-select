var EventEmitter = require('events').EventEmitter
var _            = require('lodash')
var murmurHash   = require('murmurhash-js').murmur3
var sqlParser    = require('sql-parser')

var common      = require('./common')
var RateCounter = require('./RateCounter')

// Number of milliseconds between refreshes
function calcInterval(rate) {
	return 100 + (45 * rate)
}

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
		// DEBUG HELPERS
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
									? queryBuffer.triggers[payload.table](payload.new_data[0])
										|| queryBuffer.triggers[payload.table](payload.old_data[0])
									: queryBuffer.triggers[payload.table](payload.data[0])))
							|| (queryBuffer.triggers
								// No manual trigger for this table
								&& !(payload.table in  queryBuffer.triggers))
							|| !queryBuffer.triggers) {

							if(queryBuffer.parsed !== null) {
								queryBuffer.notifications.push(payload)
							}

							this.waitingToUpdate.push(queryHash)
						}
					}
				}
			}
		})

		var performNextUpdate = () => {
			let queriesToUpdate =
				_.uniq(this.waitingToUpdate.splice(0, this.waitingToUpdate.length))
			this.refreshCount += queriesToUpdate.length
			this.refreshRate.inc(queriesToUpdate.length)

			for(let queryHash of queriesToUpdate) {
				this._updateQuery(queryHash)
			}

			setTimeout(performNextUpdate, calcInterval(this.refreshRate.rate))
		}.bind(this)
		performNextUpdate()
	}

	async select(query, params, onUpdate, triggers) {
		// Allow omission of params argument
		if(typeof params === 'function' && typeof onUpdate === 'undefined') {
			triggers = onUpdate
			onUpdate = params
			params = []
		}

		if(typeof query !== 'string')
			throw new Error('QUERY_STRING_MISSING')
		if(!(params instanceof Array))
			throw new Error('PARAMS_ARRAY_MISMATCH')
		if(typeof onUpdate !== 'function')
			throw new Error('UPDATE_FUNCTION_MISSING')

		let queryHash = murmurHash(JSON.stringify([ query, params ]))

		if(queryHash in this.selectBuffer) {
			let queryBuffer = this.selectBuffer[queryHash]

			queryBuffer.handlers.push(onUpdate)

			// Initial results from cache
			onUpdate(
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
				handlers      : [ onUpdate ],
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

			if(queryDetails.isUpdatable) {
				// Query parser does not support tab characters
				let cleanQuery = query.replace(/\t/g, ' ')
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

		let stop = async function() {
			let queryBuffer = this.selectBuffer[queryHash]

			if(queryBuffer) {
				_.pull(queryBuffer.handlers, onUpdate)

				if(queryBuffer.handlers.length === 0) {
					// No more query/params like this, remove from buffers
					delete this.selectBuffer[queryHash]
					_.pull(this.waitingToUpdate, queryHash)

					for(let table of Object.keys(this.tablesUsed)) {
						_.pull(this.tablesUsed[table], queryHash)
					}
				}
			}

		}.bind(this)

		return { stop }
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
				updateHandler(
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
