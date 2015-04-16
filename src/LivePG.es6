var EventEmitter = require('events').EventEmitter
var _            = require('lodash')
var murmurHash   = require('murmurhash-js').murmur3

var common       = require('./common')
var SelectHandle = require('./SelectHandle')

/*
 * Duration (ms) to wait to check for new updates when no updates are
 *  available in current frame
 */
const STAGNANT_TIMEOUT = 100

class LivePG extends EventEmitter {
	constructor(connStr, channel) {
		this.connStr         = connStr
		this.channel         = channel
		this.notifyHandle    = null
		this.waitingToUpdate = []
		this.selectBuffer    = {}
		this.allTablesUsed   = {}
		this.tablesUsedCache = {}

		this.ready = this.init()
		this.ready.catch(this._error)
	}

	_error(reason) {
		this.emit('error', reason)
	}

	async init() {
		this.notifyHandle = await common.getClient(this.connStr)

		common.performQuery(this.notifyHandle.client, `LISTEN "${this.channel}"`)
			.catch(this._error)

		this.notifyHandle.client.on('notification', info => {
			if(info.channel === this.channel) {
				try {
					// See common.createTableTrigger() for payload definition
					var payload = JSON.parse(info.payload)
				} catch(error) {
					return this._error(
						new Error('INVALID_NOTIFICATION ' + info.payload))
				}

				if(payload.table in this.allTablesUsed) {
					for(let queryHash of this.allTablesUsed[payload.table]) {
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
								// No manual trigger for this table, always refresh
								&& !(payload.table in  queryBuffer.triggers))
							// No manual triggers at all, always refresh
							|| !queryBuffer.triggers) {

							this.waitingToUpdate.push(queryHash)
						}
					}
				}
			}
		})

		// Initialize neverending loop to refresh active result sets
		var performNextUpdate = function() {
			if(this.waitingToUpdate.length !== 0) {
				let queriesToUpdate =
					_.uniq(this.waitingToUpdate.splice(0, this.waitingToUpdate.length))

				Promise.all(
					queriesToUpdate.map(queryHash => this._updateQuery(queryHash)))
					.then(performNextUpdate)
					.catch(this._error)
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
			.catch(this._error)

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
				notifications : []
			}

			let pgHandle = await common.getClient(this.connStr)
			let tablesUsed
			if(queryHash in this.tablesUsedCache) {
				tablesUsed = this.tablesUsedCache[queryHash]
			}
			else {
				tablesUsed = await common.getQueryDetails(pgHandle.client, query)
				this.tablesUsedCache[queryHash] = tablesUsed
			}

			for(let table of tablesUsed) {
				if(!(table in this.allTablesUsed)) {
					this.allTablesUsed[table] = [ queryHash ]
					await common.createTableTrigger(pgHandle.client, table, this.channel)
				}
				else if(this.allTablesUsed[table].indexOf(queryHash) === -1) {
					this.allTablesUsed[table].push(queryHash)
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
		let update = await common.getResultSetDiff(
			pgHandle.client,
			queryBuffer.data,
			queryBuffer.query,
			queryBuffer.params
		)

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

		for(let table of Object.keys(this.allTablesUsed)) {
			await common.dropTableTrigger(pgHandle.client, table, this.channel)
		}

		pgHandle.done()
	}
}

module.exports = LivePG
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
