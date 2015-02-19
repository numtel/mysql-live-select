var _ = require('lodash');

var cache = {};

class RowCache {
	add(key, data) {
		if(!cache[key]) {
			cache[key] = {
				data : {},
				refs : 0
			}
		}

		cache[key].data = data;
		cache[key].refs++;
	}

	remove(key) {
		if(cache[key]) {
			cache[key].refs--;

			if(!cache[key].refs) {
				delete cache[key];
			}
		}
	}

	get(key) {
		return cache[key] ? _.clone(cache[key].data) : null;
	}
}

module.exports = RowCache;
