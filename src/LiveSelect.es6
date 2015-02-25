var _            = require('lodash');
var deep         = require('deep-diff');
var EventEmitter = require('events').EventEmitter;
var murmurHash    = require('murmurhash-js').murmur3;

var querySequence = require('./querySequence');

// Minimum duration in milliseconds between refreshing results
// TODO: determine based on load
// https://git.focus-sis.com/beng/pg-notify-trigger/issues/6
const THROTTLE_INTERVAL = 1000;

class LiveSelect extends EventEmitter {
	constructor(parent, query, params) {
		var { channel } = parent;

		this.parent    = parent;
		this.query     = query;
		this.params    = params || [];
		this.ready     = false;
		this.tablesUsed = [];

		var rawHash = murmurHash(JSON.stringify([ query, params ]));
		// Adjust hash value because Postgres integers are signed
		this.queryHash = rawHash + (1 << 31);
		this.updateFunction = `${channel}_${rawHash}`;

		parent.on(this.updateFunction, this.update.bind(this));

		if(this.updateFunction in parent.resultCache){
			// This exact query has been initialized already
			this.init = parent.init
		}else{
			parent.resultCache[this.updateFunction] = [];

			this.init = new Promise((resolve, reject) => {
				parent.init.then(result => {
					querySequence(parent, [ `
							INSERT INTO
								${parent.hashTable} (query_hash, row_hashes)
							VALUES (${this.queryHash}, '{}')
						`,
						interpolate(`
						CREATE OR REPLACE FUNCTION ${this.updateFunction}()
							RETURNS TEXT AS $$
							DECLARE
								notify_data TEXT;
							BEGIN
								WITH
									cur_results AS (${query}),
									cur_hashes AS (
										SELECT
											MD5(CAST(ROW_TO_JSON(cur_results.*) AS TEXT)) AS _hash
										FROM
											cur_results),
									old_hashes AS (
										SELECT
											UNNEST(row_hashes) AS _hash
										FROM
											${parent.hashTable}
										WHERE
											query_hash = ${this.queryHash}),
									removed_hashes AS (
										SELECT * FROM old_hashes
										EXCEPT SELECT * FROM cur_hashes),
									new_data AS (
										SELECT ROW_TO_JSON(tmp3.*) AS row_json
											FROM
												(SELECT
													MD5(CAST(ROW_TO_JSON(cur_results.*) AS TEXT)) AS _hash,
													cur_results.*
												FROM cur_results) AS tmp3
										WHERE
											_hash IN (
												SELECT * FROM cur_hashes
												EXCEPT SELECT * FROM old_hashes)),
									update_hashes AS (
										UPDATE
											${parent.hashTable}
										SET
											row_hashes =
												(SELECT ARRAY_AGG(cur_hashes._hash)	FROM cur_hashes)
										WHERE
											query_hash = ${this.queryHash}),
									removed_hashes_prep AS (
										SELECT
											${this.queryHash} AS query_hash,
											JSON_AGG(removed_hashes._hash) AS removed
										FROM
											removed_hashes),
									new_data_prep AS (
										SELECT
											${this.queryHash} AS query_hash,
											JSON_AGG(new_data.row_json) AS added
										FROM
											new_data),
									joined_prep AS (
										SELECT
											removed_hashes_prep.removed,
											new_data_prep.added
										INTO
											notify_data
										FROM
											removed_hashes_prep
										JOIN new_data_prep ON
											(removed_hashes_prep.query_hash = new_data_prep.query_hash))
								SELECT
									JSON_AGG(joined_prep.*)
								FROM
									joined_prep;

								RETURN notify_data;
							END;
						$$ LANGUAGE PLPGSQL
					`, this.params),
					`SELECT ${this.updateFunction}()` ])
						.then(results => resolve(results[2].rows), reject)
				}, reject)
			}).catch(error => this.emit('error', error))
		}
	}

	update(diff) {
		this.emit('update', diff)
	}

	stop() {
		var { parent } = this;
		this.tablesUsed.forEach(table =>
			_.pull(parent.triggerTables[table].updateFunctions, this.updateFunction));
		this.removeAllListeners();
	}
}

module.exports = LiveSelect;

function filterHashProperties(diff) {
	return diff.map(event => {
		delete event[2]._hash;
		if(event.length > 3) delete event[3]._hash;
		return event;
	});
}

function interpolate(query, params) {
	if(!params || !params.length) return query;

	return query.replace(/\$(\d)/g, (match, index) => {
		var param = params[index - 1];

		if(_.isString(param)) {
			// TODO: Need to escape quotes here better!
			return `'${param.replace(/'/g, "\\'")}'`;
		}
		else if(param instanceof Date) {
			return `'${param.toISOString()}'`;
		}
		else {
			return param;
		}
	});
}
