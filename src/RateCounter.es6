
class RateCounter {
	constructor() {
		this.incrementor = 0
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

