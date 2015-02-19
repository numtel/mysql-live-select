var testCount = 0;

module.exports = {
	setUp: function(done) {
		testCount++;

		var checkReady = function() {
			if('client' in global) done();
			else setTimeout(checkReady, 30);
		};

		checkReady();
	},

	tearDown: function(done) {
		// Allow 1 second for next test to begin, otherwise quit
		var startCount = testCount;

		setTimeout(function(){
			if(startCount === testCount){
				triggers.cleanup(function(error, results) {
					if(error) throw error;

					clientDone();
					process.exit();
				});
			}
		}, 1000);

		done();
	}
};
