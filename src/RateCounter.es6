/*
 * Simple class to determine the rate of change for a number
 */
class RateCounter {
	constructor() {
		// Operations so far this second
		this.incrementor = 0
		// Operations per second, for the previous second
		this.rate = 0

		this.updateInterval = setInterval(() => {
			this.rate = this.incrementor
			this.incrementor = 0
		}, 1000)
	}

	inc(amount=1) {
		this.incrementor += amount
	}

	stop() {
		clearInterval(this.updateInterval)
	}
}

module.exports = RateCounter

