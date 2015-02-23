var _            = require('lodash');
var deep         = require('deep-diff');
var EventEmitter = require('events').EventEmitter;

var querySequence = require('./querySequence');

// Minimum duration in milliseconds between refreshing results
// TODO: determine based on load
// https://git.focus-sis.com/beng/pg-notify-trigger/issues/6
const THROTTLE_INTERVAL = 1000;

class LiveSelect extends EventEmitter {
	constructor(parent, query, params) {
		this.parent    = parent;
		this.query     = query;
		this.params    = params || [];
		this.hashes    = [];
		this.ready     = false;
		this.triggers  = null;

		this.throttledRefresh = _.debounce(this.refresh, THROTTLE_INTERVAL);

		parent.getQueryTables(this.query).then(tables => {
			this.triggers = tables.map(table => parent.createTrigger(table));

			this.triggers.forEach(trigger => {
				trigger.on('ready', () => {
					// Check if all handlers are ready
					var pending = this.triggers.filter(trigger => !trigger.ready);

					if(pending.length === 0) {
						this.ready = true;
						this.emit('ready');

						// Grab initial results
						this.refresh();
					}

				});

				trigger.on('change', this.throttledRefresh.bind(this));
			});
		}, error => this.emit('error', error));

	}

	refresh() {
		var { parent } = this;

		var hashQueryPart = fullRow => `
			SELECT
				MD5(
					CAST(tmp.* AS TEXT) ||
					'${_.pluck(this.triggers, "table").join(",")}'
				) AS _hash
				${fullRow ? ', tmp.*' : ''}
			FROM
				tmp
		`;

		// Run a query to get an updated hash map
		var newHashesQuery = [[ `
			WITH
				tmp AS (${this.query})
			SELECT
				tmp2._hash
			FROM (${hashQueryPart(false)}) tmp2
		`, this.params ]];

		querySequence(parent, newHashesQuery).then(result => {
			var freshHashes = _.pluck(result[0].rows, '_hash');
			var diff   = deep.diff(this.hashes, freshHashes);
			var fetch  = {};

			// Store the new hash map
			this.hashes = freshHashes;

			// If nothing has changed, stop here
			if(!diff || !diff.length) {
				return;
			}

			// Build a list of changes and hashes to fetch
			var changes = diff.map(change => {
				var tmpChange = {};

				if(change.kind === 'E') {
					_.extend(tmpChange, {
						type   : 'changed',
						index  : change.path.pop(),
						oldKey : change.lhs,
						newKey : change.rhs
					});

					if(parent.rowCache.get(tmpChange.oldKey) === null) {
						fetch[tmpChange.oldKey] = true;
					}

					if(parent.rowCache.get(tmpChange.newKey) === null) {
						fetch[tmpChange.newKey] = true;
					}
				}
				else if(change.kind === 'A') {
					_.extend(tmpChange, {
						index : change.index
					})

					if(change.item.kind === 'N') {
						tmpChange.type = 'added';
						tmpChange.key  = change.item.rhs;
					}
					else {
						tmpChange.type = 'removed';
						tmpChange.key  = change.item.lhs;
					}

					if(parent.rowCache.get(tmpChange.key) === null) {
						fetch[tmpChange.key] = true;
					}
				}
				else {
					throw new Error(`Unrecognized change: ${JSON.stringify(change)}`);
				}

				return tmpChange;
			});

			if(_.isEmpty(fetch)) {
				this.update(changes);
			}
			else {
				// Fetch hashes that aren't in the cache
				var newCacheDataQuery = [[ `
					WITH
						tmp AS (${this.query})
					SELECT
						tmp2.*
					FROM
						(${hashQueryPart(true)}) tmp2
					WHERE
						tmp2._hash IN ('${_.keys(fetch).join("', '")}')
				`, this.params ]];

				querySequence(parent, newCacheDataQuery).then(result => {
					result[0].rows.forEach(row => parent.rowCache.add(row._hash, row));
					this.update(changes);
				}, error => this.emit('error', error));
			}
		}, error => this.emit('error', error));
	}

	update(changes) {
		var { parent } = this;
		var remove = [];

		// Emit an update event with the changes
		var changes = changes.map(change => {
			var args = [change.type];

			if(change.type === 'added') {
				var row = parent.rowCache.get(change.key);
				args.push(change.index, row);
			}
			else if(change.type === 'changed') {
				var oldRow = parent.rowCache.get(change.oldKey);
				var newRow = parent.rowCache.get(change.newKey);
				args.push(change.index, oldRow, newRow);
				remove.push(change.oldKey);
			}
			else if(change.type === 'removed') {
				var row = parent.rowCache.get(change.key);
				args.push(change.index, row);
				remove.push(change.key);
			}

			if(args[2] === null){
				return this.emit('error', new Error(
					'CACHE_MISS: ' + (args.length === 3 ? change.key : change.oldKey)));
			}
			if(args.length > 3 && args[3] === null){
				return this.emit('error', new Error('CACHE_MISS: ' + change.newKey));
			}

			return args;
		});

		remove.forEach(key => parent.rowCache.remove(key));

		this.emit('update', filterHashProperties(changes));
	}

	stop() {
		var { parent } = this;
		this.hashes.forEach(key => parent.rowCache.remove(key));
		this.triggers.forEach(trigger => trigger.removeAllListeners());
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
