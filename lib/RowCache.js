"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var _ = require("lodash");

var RowCache = (function () {
	function RowCache() {
		_classCallCheck(this, RowCache);

		this.cache = {};
	}

	_prototypeProperties(RowCache, null, {
		add: {
			value: function add(key, data) {
				var _ref = this;
				var cache = _ref.cache;
				if (!(key in cache)) {
					cache[key] = {
						data: {},
						refs: 0
					};
				}

				cache[key].data = data;
				cache[key].refs++;
			},
			writable: true,
			configurable: true
		},
		remove: {
			value: function remove(key) {
				var _ref = this;
				var cache = _ref.cache;
				if (key in cache) {
					cache[key].refs--;

					if (cache[key].refs === 0) {
						delete cache[key];
					}
				}
			},
			writable: true,
			configurable: true
		},
		get: {
			value: function get(key) {
				var _ref = this;
				var cache = _ref.cache;
				return key in cache ? _.clone(cache[key].data) : null;
			},
			writable: true,
			configurable: true
		}
	});

	return RowCache;
})();

module.exports = RowCache;