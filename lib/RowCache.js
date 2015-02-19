"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var _ = require("lodash");

var cache = {};

var RowCache = (function () {
	function RowCache() {
		_classCallCheck(this, RowCache);
	}

	_prototypeProperties(RowCache, null, {
		add: {
			value: function add(key, data) {
				if (!cache[key]) {
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
				if (cache[key]) {
					cache[key].refs--;

					if (!cache[key].refs) {
						delete cache[key];
					}
				}
			},
			writable: true,
			configurable: true
		},
		get: {
			value: function get(key) {
				return cache[key] ? _.clone(cache[key].data) : null;
			},
			writable: true,
			configurable: true
		}
	});

	return RowCache;
})();

module.exports = RowCache;