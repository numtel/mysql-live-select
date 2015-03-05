var _            = require('lodash');
var EventEmitter = require('events').EventEmitter;
var murmurHash    = require('murmurhash-js').murmur3;

var querySequence = require('./querySequence');

class LiveSelect extends EventEmitter {
	constructor(parent, query, params) {
		var { channel } = parent;

		this.parent    = parent;
		this.query     = query;
		this.params    = params || [];
		this.ready     = false;

		var rawHash = murmurHash(JSON.stringify([ query, params ]));
		// Adjust hash value because Postgres integers are signed
		this.queryHash = rawHash + (1 << 31);
		this.updateFunction = `${channel}_${rawHash}`;

		this.boundUpdate = this.update.bind(this);

		parent.on(this.updateFunction, this.boundUpdate);

		if(this.updateFunction in parent.resultCache){
			// This exact query has been initialized already
			var thisCache = parent.resultCache[this.updateFunction];
			this.init = thisCache.init;

			// Send initial results from cache if available
			if(thisCache.data.length > 0) {
				this.update(
					{ removed: null, moved: null, added: thisCache.data },
					thisCache.data);
			}
		}else{
			this.init = new Promise((resolve, reject) => {
				parent.init.then(result => {
					parent.registerQueryTriggers(this.query, this.updateFunction)
						.then(() => {
							// Get initial results
							parent.waitingToUpdate.push(this.updateFunction);

							resolve()
						}, reject);
				})
			}, error => this.emit('error', error));

			parent.resultCache[this.updateFunction] = {
				data   : [],
				init   : this.init,
				query  : this.query,
				params : this.params
			};
		}
	}

	update(diff, rows) {
		this.ready = true;
		this.emit('update', filterHashProperties(diff), filterHashProperties(rows))
	}

	stop() {
		var { parent } = this;

		this.removeAllListeners();
		parent.removeListener(this.updateFunction, this.boundUpdate);

	}
}

module.exports = LiveSelect;

/**
 * @param Array|Object diff If object, all values must be arrays
 */
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
