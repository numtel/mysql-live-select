"use strict";

var _classCallCheck = require("babel-runtime/helpers/class-call-check")["default"];

var _inherits = require("babel-runtime/helpers/inherits")["default"];

var _createClass = require("babel-runtime/helpers/create-class")["default"];

var _core = require("babel-runtime/core-js")["default"];

var EventEmitter = require("events").EventEmitter;

var _ = require("lodash");

var SelectHandle = (function (_EventEmitter) {
	function SelectHandle(parent, queryHash) {
		_classCallCheck(this, SelectHandle);

		this.parent = parent;
		this.queryHash = queryHash;
	}

	_inherits(SelectHandle, _EventEmitter);

	_createClass(SelectHandle, {
		stop: {
			value: function stop() {
				var _ref = this;

				var parent = _ref.parent;
				var queryHash = _ref.queryHash;

				var queryBuffer = parent.selectBuffer[queryHash];

				if (queryBuffer) {
					_.pull(queryBuffer.handlers, this);

					if (queryBuffer.handlers.length === 0) {
						// No more query/params like this, remove from buffers
						delete parent.selectBuffer[queryHash];
						_.pull(parent.waitingToUpdate, queryHash);

						var _iteratorNormalCompletion = true;
						var _didIteratorError = false;
						var _iteratorError = undefined;

						try {
							for (var _iterator = _core.$for.getIterator(_core.Object.keys(parent.tablesUsed)), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
								var table = _step.value;

								_.pull(parent.tablesUsed[table], queryHash);
							}
						} catch (err) {
							_didIteratorError = true;
							_iteratorError = err;
						} finally {
							try {
								if (!_iteratorNormalCompletion && _iterator["return"]) {
									_iterator["return"]();
								}
							} finally {
								if (_didIteratorError) {
									throw _iteratorError;
								}
							}
						}
					}
				}
			}
		}
	});

	return SelectHandle;
})(EventEmitter);

module.exports = SelectHandle;