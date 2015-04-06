"use strict";

var _classCallCheck = require("babel-runtime/helpers/class-call-check")["default"];

var _createClass = require("babel-runtime/helpers/create-class")["default"];

var RateCounter = (function () {
	function RateCounter() {
		var _this = this;

		_classCallCheck(this, RateCounter);

		this.incrementor = 0;
		this.rate = 0;

		this.updateInterval = setInterval(function () {
			_this.rate = _this.incrementor;
			_this.incrementor = 0;
		}, 1000);
	}

	_createClass(RateCounter, {
		inc: {
			value: function inc() {
				var amount = arguments[0] === undefined ? 1 : arguments[0];

				this.incrementor += amount;
			}
		},
		stop: {
			value: function stop() {
				clearInterval(this.updateInterval);
			}
		}
	});

	return RateCounter;
})();

module.exports = RateCounter;