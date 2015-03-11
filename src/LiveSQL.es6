var EventEmitter = require('events').EventEmitter
var _            = require('lodash')
var murmurHash   = require('murmurhash-js').murmur3

var common     = require('./common')

// Number of milliseconds between refreshes
const THROTTLE_INTERVAL = 100

class LiveSQL extends EventEmitter {
	constructor(connStr, channel) {
		this.connStr         = connStr
		this.channel         = channel
		this.notifyHandle    = null
		this.updateInterval  = null
		this.waitingToUpdate = []
		this.selectBuffer    = {}
		this.tablesUsed      = {}

		this.ready = this.init()
	}

	async init() {
		this.notifyHandle = await common.getClient(this.connStr)

		await common.performQuery(this.notifyHandle.client,
			`LISTEN "${this.channel}"`)

		this.notifyHandle.client.on('notification', info => {
			if(info.channel === this.channel && info.payload in this.tablesUsed){
				this.waitingToUpdate =
					_.union(this.waitingToUpdate, this.tablesUsed[info.payload])
			}
		})

		this.updateInterval = setInterval(() => {
			let queriesToUpdate =
				_.uniq(this.waitingToUpdate.splice(0, this.waitingToUpdate.length))

			for(let queryHash of queriesToUpdate) {
				this._updateQuery(queryHash)
			}
		}.bind(this), THROTTLE_INTERVAL)
	}

	async select(query, params, onUpdate) {
		// Allow omission of params argument
		if(typeof params === 'function' && typeof onUpdate === 'undefined') {
			onUpdate = params
			params = []
		}

		if(typeof query !== 'string')
			throw new Error('QUERY_STRING_MISSING')
		if(!(params instanceof Array))
			throw new ERROR('PARAMS_ARRAY_MISMATCH')
		if(typeof onUpdate !== 'function')
			throw new Error('UPDATE_FUNCTION_MISSING')

		let queryHash = murmurHash(JSON.stringify([ query, params ]))

		if(queryHash in this.selectBuffer) {
			let queryBuffer = this.selectBuffer[queryHash]

			queryBuffer.handlers.push(onUpdate)

			if(bufferData.length !== 0) {
				// Initial results from cache
				onUpdate(
					{ removed: null, moved: null, copied: null, added: queryBuffer.data },
					queryBuffer.data)
			}
		}
		else {
			// Initialize result set cache
			this.selectBuffer[queryHash] = {
				query,
				params,
				data     : [],
				handlers : [ onUpdate ]
			}

			let pgHandle = await common.getClient(this.connStr)
			let queryTables = await common.getQueryTables(pgHandle.client, query)

			for(let table of queryTables) {
				if(!(table in this.tablesUsed)) {
					this.tablesUsed[table] = [ queryHash ]
					await common.createTableTrigger(pgHandle.client, table, this.channel)
				}
				else if(this.tablesUsed[table].indexOf(queryHash) === -1) {
					this.tablesUsed[table].push(queryHash)
				}
			}

			pgHandle.done()
			pgHandle = null
			queryTables = null

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

			stop = null
			queryHash = null
		}.bind(this)

		return { stop }
	}

	async _updateQuery(queryHash) {
		let pgHandle = await common.getClient(this.connStr)

		let queryBuffer = this.selectBuffer[queryHash]
		let diff = await common.getResultSetDiff(
			pgHandle.client,
			queryBuffer.data,
			queryBuffer.query,
			queryBuffer.params)

		pgHandle.done()
		pgHandle = null

		if(diff !== null) {
			queryBuffer.data = common.applyDiff(queryBuffer.data, diff)

			for(let updateHandler of queryBuffer.handlers) {
				updateHandler(
					filterHashProperties(diff), filterHashProperties(queryBuffer.data))
			}
		}
	}

	async cleanup() {
		this.notifyHandle.done()
		this.notifyHandle = null

		clearInterval(this.updateInterval)
		this.updateInterval = null

		let pgHandle = await common.getClient(this.connStr)
		
		for(let table of Object.keys(this.tablesUsed)) {
			await common.dropTableTrigger(pgHandle.client, table, this.channel)
		}

		pgHandle.done()
		pgHandle = null
	}
}

module.exports = LiveSQL

function filterHashProperties(diff) {
	if(diff instanceof Array) {
		return diff.map(event => {
			return _.omit(event, '_hash');
		});
	}else{
		_.forOwn(diff, (rows, key) => {
			diff[key] = filterHashProperties(rows)
		})
	}
	return diff;
}
