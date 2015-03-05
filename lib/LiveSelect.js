"use strict";

var _babelHelpers = require("babel-runtime/helpers")["default"];

var _core = require("babel-runtime/core-js")["default"];

var _ = require("lodash");
var EventEmitter = require("events").EventEmitter;
var murmurHash = require("murmurhash-js").murmur3;

var querySequence = require("./querySequence");

var LiveSelect = (function (EventEmitter) {
	function LiveSelect(parent, query, params) {
		var _this = this;

		_babelHelpers.classCallCheck(this, LiveSelect);

		var channel = parent.channel;

		this.parent = parent;
		this.query = query;
		this.params = params || [];
		this.ready = false;

		var rawHash = murmurHash(JSON.stringify([query, params]));
		// Adjust hash value because Postgres integers are signed
		this.queryHash = rawHash + (1 << 31);
		this.updateFunction = "" + channel + "_" + rawHash;

		this.boundUpdate = this.update.bind(this);

		parent.on(this.updateFunction, this.boundUpdate);

		if (this.updateFunction in parent.resultCache) {
			// This exact query has been initialized already
			var thisCache = parent.resultCache[this.updateFunction];
			this.init = thisCache.init;

			// Send initial results from cache if available
			if (thisCache.data.length > 0) {
				this.update({ removed: null, moved: null, added: thisCache.data }, thisCache.data);
			}
		} else {
			this.init = new _core.Promise(function (resolve, reject) {
				parent.init.then(function (result) {
					parent.registerQueryTriggers(_this.query, _this.updateFunction).then(function () {
						// Get initial results
						parent.waitingToUpdate.push(_this.updateFunction);

						resolve();
					}, reject);
				});
			}, function (error) {
				return _this.emit("error", error);
			});

			parent.resultCache[this.updateFunction] = {
				data: [],
				init: this.init,
				query: this.query,
				params: this.params
			};
		}
	}

	_babelHelpers.inherits(LiveSelect, EventEmitter);

	_babelHelpers.prototypeProperties(LiveSelect, null, {
		update: {
			value: function update(diff, rows) {
				this.ready = true;
				this.emit("update", filterHashProperties(diff), filterHashProperties(rows));
			},
			writable: true,
			configurable: true
		},
		stop: {
			value: function stop() {
				var _ref = this;

				var parent = _ref.parent;

				this.removeAllListeners();
				parent.removeListener(this.updateFunction, this.boundUpdate);
			},
			writable: true,
			configurable: true
		}
	});

	return LiveSelect;
})(EventEmitter);

module.exports = LiveSelect;

/**
 * @param Array|Object diff If object, all values must be arrays
 */
function filterHashProperties(diff) {
	if (diff instanceof Array) {
		return diff.map(function (event) {
			return _.omit(event, "_hash");
		});
	} else {
		_.forOwn(diff, function (rows, key) {
			diff[key] = filterHashProperties(rows);
		});
	}
	return diff;
}