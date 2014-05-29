module.exports = function(host, port) {
	host = host || 'localhost';
	port = port || 11300;
	
	var publ        = this,
		priv        = {},

		os          = require('os'),
		uuid        = require('node-uuid').v4,
		async       = require('async'),
		fivebeans   = require('fivebeans'),

		connected           = false,

		workClient          = null,
		workQueue           = [],
		workRunning         = false,

		replyClient         = null,
		replyQueue          = [],
		replyRunning        = false,

		listeningForReplies = false,
		replyTTR            = 1000,

		defaultTube         = 'work',
		replyTube           = os.hostname() + '-' + process.pid + '-reply',

		callbacks           = {};

	priv.generateId = uuid;

	priv.parsePayload = function(payload) {
		return JSON.parse(payload.toString());
	}

	priv.stringifyData = function(data) {
		return JSON.stringify(data);
	}

	priv.connect = function(callback) {
		if (connected) {
			return callback(null);
		}

		workClient	= new fivebeans.client(host, port);
		replyClient	= new fivebeans.client(host, port);

		async.parallel([
			function(callback) {
				workClient
					.on('connect',  callback)
					.on('error',    callback)
					.on('close',    function(err) { connected = false; })
					.connect();
			},
			function(callback) {
				replyClient
					.on('connect',  callback)
					.on('error',    callback)
					.on('close',    function(err) { connected = false; })
					.connect();
			}
		], function(err) {
			if (err) {
				connected = false;
				listeningForReplies	= false;
				return callback(err);
			}

			connected = true;
			callback(null);
		});
	}

	priv.registerReplyCallback = function(callback) {
		var id = priv.generateId();
		callbacks[id] = callback;

		return id;
	}

	priv.executeReplyCallback = function(err, id, finished, payload) {
		var callback = callbacks[id];
		if (id && callback) {
			if (err) return callback(err);

			callback(null, finished, payload);
		}
	}

	priv.listenForReplies = function() {
		if (!connected) listeningForReplies = false;

		if (!listeningForReplies) {
			priv.connect(function(err) {
				if (err) return priv.listenForReplies();

				listeningForReplies = true;

				priv.reserveReply(function(err) {
					listeningForReplies = false;
					priv.listenForReplies();
				})

			});
		}
	}

	priv.runReply = function(fn) {
		if (!replyRunning) {
			fn();
		}
		else {
			priv.pushReplyQueue(fn);
		}
	}

	priv.pushReplyQueue = function(work) {
		replyQueue.push(work);
	};

	priv.popReplyQueue = function() {
		(replyQueue.pop() || function() {})();
	};

	priv.reserveReply = function(successCallback) {
		successCallback = successCallback || function() {};

		var callback = function(err) {
			replyRunning = false;
			priv.popReplyQueue();
			successCallback(err);
		};

		priv.connect(function(err) {
			if (err) return callback(err);

			priv.runReply(function() {
				replyRunning = true;

				replyClient.watch(replyTube, function(err) {
					if (err) return callback(err);

					replyClient.reserve(function(err, jobid, payload) {
						if (err) return callback(err);

						var data = priv.parsePayload(payload);

					    priv.executeReplyCallback(null, data.callbackId, data.finished, data.payload);

						replyClient.destroy(jobid, callback);
					});
				});
			});
		});
	}

	priv.reply = function(replyTo, callbackId, finished, payload, successCallback) {
		successCallback = successCallback || function() {};

		var callback = function(err) {
			replyRunning = false;
			priv.popReplyQueue();
			successCallback(err);
		};

		priv.connect(function(err) {
			if (err) return callback(err);

			priv.runReply(function() {
				replyRunning = true;
				var data = {
					tube:   	replyTo,
					callbackId: callbackId,
					finished:   finished,
					payload: 	payload
				}

				replyClient.use(data.tube, function(err, tubename) {
					if (err) return callback(err);

					replyClient.put(
						0,
						0,
						replyTTR,
						priv.stringifyData(data),
						callback
					);
				})
			});
		});
	}

	priv.runWork = function(fn) {
		if (!workRunning) {
			fn();
		}
		else {
			priv.pushWorkQueue(fn);
		}
	}

	priv.pushWorkQueue = function(work) {
		workQueue.push(work);
	};

	priv.popWorkQueue = function() {
		(workQueue.pop() || function() {})();
	};

	publ.put = function(tube, payload, successCallback, replyCallback, priority, delay, ttr) {
		tube            = tube || defaultTube
		priority        = priority || 0;
		delay           = delay || 0;
		ttr             = ttr || 1000;
		successCallback = successCallback || function() {};


		var callback = function(err) {
			workRunning = false;
			priv.popWorkQueue();
			successCallback(err);
		};

		priv.connect(function(err) {
			if (err) return callback(err);

			priv.runWork(function() {
				workRunning = true;

				var data = {
					tube: 		tube,
					payload: 	payload
				};

				if (replyCallback) {
					data.replyTo	= replyTube;
					data.callbackId	= priv.registerReplyCallback(replyCallback);

					priv.listenForReplies();
				}

				workClient.use(tube, function(err, tubename) {
					if (err) return callback(err);

					workClient.put(
						priority,
						delay,
						ttr,
						priv.stringifyData(data),
						callback
					);
				});
			})
		});
	}


	publ.reserve = function(tube, successCallback) {
		tube            = tube || defaultTube;
		successCallback = successCallback || function() {};

		var callback = function(err) {
			workRunning = false;
			priv.popWorkQueue();
			successCallback(err);
		};

		priv.connect(function(err) {
			if (err) return callback(err);

			priv.runWork(function() {
				workRunning = true;

				workClient.watch(tube, function(err, numwatched) {
					if (err) return callback(err);

					workClient.reserve(function(err, jobid, payload) {
						workRunning = false;

						if (err) return successCallback(err);

						var data = priv.parsePayload(payload);

						successCallback(
							null,
							data.payload,
							priv.generateReplyCallback(jobid, payload)
						);

						priv.popWorkQueue();
					});
				});
			});
		});
	}

	priv.generateReplyCallback = function(jobid, payload) {
		return function(finished, replyPayload, successCallback) {
			successCallback = successCallback || function() {};

			var reply = function(err) {
				if (err) return successCallback(err);

				var data = priv.parsePayload(payload);

				if (data.replyTo) {
					priv.reply(
						data.replyTo,
						data.callbackId,
						finished,
						replyPayload,
						successCallback
					)
				}
				else {
					successCallback(null);
				}
			}

			if (finished) {
				priv.runWork(function() {
					workRunning = true;
					workClient.destroy(jobid, function(err) {
						workRunning = false;
						priv.popWorkQueue();
						reply(err);
					});
				});
			}
			else {
				reply(null);
			}
		}
	}
}