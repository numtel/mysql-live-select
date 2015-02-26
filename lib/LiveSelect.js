"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var _ = require("lodash");
var EventEmitter = require("events").EventEmitter;
var murmurHash = require("murmurhash-js").murmur3;

var querySequence = require("./querySequence");

var LiveSelect = (function (EventEmitter) {
	function LiveSelect(parent, query, params) {
		var _this = this;
		_classCallCheck(this, LiveSelect);

		var channel = parent.channel;


		this.parent = parent;
		this.query = query;
		this.params = params || [];
		this.ready = false;
		this.tablesUsed = [];

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
			// TODO possible to move diffing to separate, global function?
			// TODO possible to replace IN clauses with INNER JOINs?
			this.init = new Promise(function (resolve, reject) {
				parent.init.then(function (result) {
					querySequence(parent, ["\n\t\t\t\t\t\t\tINSERT INTO\n\t\t\t\t\t\t\t\t" + parent.hashTable + " (query_hash, row_hashes)\n\t\t\t\t\t\t\tVALUES (" + _this.queryHash + ", '{}')\n\t\t\t\t\t\t", interpolate("\n\t\t\t\t\t\tCREATE OR REPLACE FUNCTION " + _this.updateFunction + "()\n\t\t\t\t\t\t\tRETURNS TEXT AS $$\n\t\t\t\t\t\t\tDECLARE\n\t\t\t\t\t\t\t\tnotify_data TEXT;\n\t\t\t\t\t\t\tBEGIN\n\t\t\t\t\t\t\t\tWITH\n\t\t\t\t\t\t\t\t\tcur_results AS (" + query + "),\n\t\t\t\t\t\t\t\t\tcur_hashes AS (\n\t\t\t\t\t\t\t\t\t\tSELECT\n\t\t\t\t\t\t\t\t\t\t\tROW_NUMBER() OVER () AS _index,\n\t\t\t\t\t\t\t\t\t\t\tMD5(\n\t\t\t\t\t\t\t\t\t\t\t\tCAST(ROW_TO_JSON(cur_results.*) AS TEXT) ||\n\t\t\t\t\t\t\t\t\t\t\t\tCAST(ROW_NUMBER() OVER () AS TEXT)\n\t\t\t\t\t\t\t\t\t\t\t) AS _hash\n\t\t\t\t\t\t\t\t\t\tFROM\n\t\t\t\t\t\t\t\t\t\t\tcur_results),\n\t\t\t\t\t\t\t\t\told_hashes AS (\n\t\t\t\t\t\t\t\t\t\tSELECT\n\t\t\t\t\t\t\t\t\t\t\tROW_NUMBER() OVER () AS _index,\n\t\t\t\t\t\t\t\t\t\t\ttmp._hash\n\t\t\t\t\t\t\t\t\t\tFROM\n\t\t\t\t\t\t\t\t\t\t\t(SELECT\n\t\t\t\t\t\t\t\t\t\t\t\tUNNEST(row_hashes) AS _hash\n\t\t\t\t\t\t\t\t\t\t\tFROM\n\t\t\t\t\t\t\t\t\t\t\t\t" + parent.hashTable + "\n\t\t\t\t\t\t\t\t\t\t\tWHERE\n\t\t\t\t\t\t\t\t\t\t\t\tquery_hash = " + _this.queryHash + ") AS tmp),\n\t\t\t\t\t\t\t\t\tchanged_hashes AS (\n\t\t\t\t\t\t\t\t\t\tSELECT * FROM old_hashes\n\t\t\t\t\t\t\t\t\t\tEXCEPT SELECT * FROM cur_hashes),\n\t\t\t\t\t\t\t\t\tmoved_hashes AS (\n\t\t\t\t\t\t\t\t\t\tSELECT\n\t\t\t\t\t\t\t\t\t\t\tcur_hashes._index AS new_index,\n\t\t\t\t\t\t\t\t\t\t\told_hashes._index AS old_index,\n\t\t\t\t\t\t\t\t\t\t\tcur_hashes._hash AS _hash\n\t\t\t\t\t\t\t\t\t\tFROM\n\t\t\t\t\t\t\t\t\t\t\tcur_hashes\n\t\t\t\t\t\t\t\t\t\tJOIN\n\t\t\t\t\t\t\t\t\t\t\told_hashes ON\n\t\t\t\t\t\t\t\t\t\t\t\t(old_hashes._hash = cur_hashes._hash)\n\t\t\t\t\t\t\t\t\t\tINNER JOIN\n\t\t\t\t\t\t\t\t\t\t\tchanged_hashes ON\n\t\t\t\t\t\t\t\t\t\t\t\t(changed_hashes._hash = cur_hashes._hash)),\n\t\t\t\t\t\t\t\t\tremoved_hashes AS (\n\t\t\t\t\t\t\t\t\t\tSELECT * FROM changed_hashes WHERE _hash NOT IN\n\t\t\t\t\t\t\t\t\t\t\t(SELECT _hash FROM moved_hashes)),\n\t\t\t\t\t\t\t\t\tnew_data AS (\n\t\t\t\t\t\t\t\t\t\tSELECT ROW_TO_JSON(tmp3.*) AS row_json\n\t\t\t\t\t\t\t\t\t\t\tFROM\n\t\t\t\t\t\t\t\t\t\t\t\t(SELECT\n\t\t\t\t\t\t\t\t\t\t\t\t\tMD5(\n\t\t\t\t\t\t\t\t\t\t\t\t\t\tCAST(ROW_TO_JSON(cur_results.*) AS TEXT) ||\n\t\t\t\t\t\t\t\t\t\t\t\t\t\tCAST(ROW_NUMBER() OVER () AS TEXT)\n\t\t\t\t\t\t\t\t\t\t\t\t\t) AS _hash,\n\t\t\t\t\t\t\t\t\t\t\t\t\tROW_NUMBER() OVER () AS _index,\n\t\t\t\t\t\t\t\t\t\t\t\t\tcur_results.*\n\t\t\t\t\t\t\t\t\t\t\t\tFROM cur_results) AS tmp3\n\t\t\t\t\t\t\t\t\t\tWHERE\n\t\t\t\t\t\t\t\t\t\t\t_hash IN (\n\t\t\t\t\t\t\t\t\t\t\t\tSELECT _hash FROM cur_hashes\n\t\t\t\t\t\t\t\t\t\t\t\tEXCEPT SELECT _hash FROM old_hashes)),\n\t\t\t\t\t\t\t\t\tupdate_hashes AS (\n\t\t\t\t\t\t\t\t\t\tUPDATE\n\t\t\t\t\t\t\t\t\t\t\t" + parent.hashTable + "\n\t\t\t\t\t\t\t\t\t\tSET\n\t\t\t\t\t\t\t\t\t\t\trow_hashes =\n\t\t\t\t\t\t\t\t\t\t\t\t(SELECT ARRAY_AGG(cur_hashes._hash)\tFROM cur_hashes)\n\t\t\t\t\t\t\t\t\t\tWHERE\n\t\t\t\t\t\t\t\t\t\t\tquery_hash = " + _this.queryHash + "),\n\t\t\t\t\t\t\t\t\tremoved_hashes_prep AS (\n\t\t\t\t\t\t\t\t\t\tSELECT\n\t\t\t\t\t\t\t\t\t\t\t" + _this.queryHash + " AS query_hash,\n\t\t\t\t\t\t\t\t\t\t\tJSON_AGG(removed_hashes.*) AS removed\n\t\t\t\t\t\t\t\t\t\tFROM\n\t\t\t\t\t\t\t\t\t\t\tremoved_hashes),\n\t\t\t\t\t\t\t\t\tmoved_hashes_prep AS (\n\t\t\t\t\t\t\t\t\t\tSELECT\n\t\t\t\t\t\t\t\t\t\t\t" + _this.queryHash + " AS query_hash,\n\t\t\t\t\t\t\t\t\t\t\tJSON_AGG(moved_hashes.*) AS moved\n\t\t\t\t\t\t\t\t\t\tFROM\n\t\t\t\t\t\t\t\t\t\t\tmoved_hashes),\n\t\t\t\t\t\t\t\t\tnew_data_prep AS (\n\t\t\t\t\t\t\t\t\t\tSELECT\n\t\t\t\t\t\t\t\t\t\t\t" + _this.queryHash + " AS query_hash,\n\t\t\t\t\t\t\t\t\t\t\tJSON_AGG(new_data.row_json) AS added\n\t\t\t\t\t\t\t\t\t\tFROM\n\t\t\t\t\t\t\t\t\t\t\tnew_data),\n\t\t\t\t\t\t\t\t\tjoined_prep AS (\n\t\t\t\t\t\t\t\t\t\tSELECT\n\t\t\t\t\t\t\t\t\t\t\tremoved_hashes_prep.removed,\n\t\t\t\t\t\t\t\t\t\t\tmoved_hashes_prep.moved,\n\t\t\t\t\t\t\t\t\t\t\tnew_data_prep.added\n\t\t\t\t\t\t\t\t\t\tINTO\n\t\t\t\t\t\t\t\t\t\t\tnotify_data\n\t\t\t\t\t\t\t\t\t\tFROM\n\t\t\t\t\t\t\t\t\t\t\tremoved_hashes_prep\n\t\t\t\t\t\t\t\t\t\tJOIN new_data_prep ON\n\t\t\t\t\t\t\t\t\t\t\t(removed_hashes_prep.query_hash =\n\t\t\t\t\t\t\t\t\t\t\t\tnew_data_prep.query_hash)\n\t\t\t\t\t\t\t\t\t\tJOIN moved_hashes_prep ON\n\t\t\t\t\t\t\t\t\t\t\t(removed_hashes_prep.query_hash =\n\t\t\t\t\t\t\t\t\t\t\t\tmoved_hashes_prep.query_hash))\n\t\t\t\t\t\t\t\tSELECT\n\t\t\t\t\t\t\t\t\tJSON_AGG(joined_prep.*)\n\t\t\t\t\t\t\t\tFROM\n\t\t\t\t\t\t\t\t\tjoined_prep;\n\n\t\t\t\t\t\t\t\tRETURN notify_data;\n\t\t\t\t\t\t\tEND;\n\t\t\t\t\t\t$$ LANGUAGE PLPGSQL\n\t\t\t\t\t", _this.params)]).then(resolve, reject);
				}, reject);
			}).then(function (result) {
				// Get initial results
				parent.waitingToUpdate.push(_this.updateFunction);

				parent.registerQueryTriggers(_this.query, _this.updateFunction).then(function (tables) {
					_this.tablesUsed = tables;
				});
			}, function (error) {
				return _this.emit("error", error);
			});

			parent.resultCache[this.updateFunction] = { data: [], init: this.init };
		}
	}

	_inherits(LiveSelect, EventEmitter);

	_prototypeProperties(LiveSelect, null, {
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
				var _this = this;
				var _ref = this;
				var parent = _ref.parent;


				this.removeAllListeners();
				parent.removeListener(this.updateFunction, this.boundUpdate);

				// Asynchronously, remove trigger if no longer used
				return new Promise(function (resolve, reject) {
					setTimeout(function () {
						if (parent.listeners(_this.updateFunction).length === 0) {
							// Remove listing from parent
							_this.tablesUsed.forEach(function (table) {
								return _.pull(parent.triggerTables[table].updateFunctions, _this.updateFunction);
							});

							// Remove from database
							querySequence(parent, ["DROP FUNCTION IF EXISTS " + _this.updateFunction + "()"]).then(resolve, reject);
						} else {
							resolve();
						}
					}, 100);
				});
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
			delete event._hash;
			return event;
		});
	} else {
		_.forOwn(diff, function (rows, key) {
			diff[key] = filterHashProperties(rows);
		});
	}
	return diff;
}

function interpolate(query, params) {
	if (!params || !params.length) return query;

	return query.replace(/\$(\d)/g, function (match, index) {
		var param = params[index - 1];

		if (_.isString(param)) {
			// TODO: Need to escape quotes here better!
			return "'" + param.replace(/'/g, "\\'") + "'";
		} else if (param instanceof Date) {
			return "'" + param.toISOString() + "'";
		} else {
			return param;
		}
	});
}