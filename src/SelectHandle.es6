var EventEmitter = require('events').EventEmitter
var _ = require('lodash')

class SelectHandle extends EventEmitter {
	constructor(parent, queryHash) {
		this.parent    = parent
		this.queryHash = queryHash
	}

	stop() {
		let { parent, queryHash } = this
		let queryBuffer = parent.selectBuffer[queryHash]

		if(queryBuffer) {
			_.pull(queryBuffer.handlers, this)

			if(queryBuffer.handlers.length === 0) {
				// No more query/params like this, remove from buffers
				delete parent.selectBuffer[queryHash]
				_.pull(parent.waitingToUpdate, queryHash)

				for(let table of Object.keys(parent.allTablesUsed)) {
					_.pull(parent.allTablesUsed[table], queryHash)
				}
			}
		}
	}
}

module.exports = SelectHandle

