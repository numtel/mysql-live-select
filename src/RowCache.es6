var _ = require('lodash');

class RowCache {
	constructor() {
		this.cache = {};
	}

	add(key, data) {
		var { cache } = this;
		if(!(key in cache)) {
			cache[key] = {
				data : {},
				refs : 0
			}
		}

		cache[key].data = data;
		cache[key].refs++;
	}

	remove(key) {
		var { cache } = this;
		if(key in cache) {
			cache[key].refs--;

			if(cache[key].refs === 0) {
				delete cache[key];
			}
		}
	}

	get(key) {
		var { cache } = this;
		return key in cache ? _.clone(cache[key].data) : null;
	}
}

module.exports = RowCache;
