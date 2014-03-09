module.exports = function() {
	var publ		= this,
		priv		= {},

		os			= require('os'),
		uuid 		= require('node-uuid').v4;
		async 		= require('async');
		fivebeans 	= require('fivebeans'),

		workClient			= new fivebeans.client('localhost', 11300),
		replyClient			= new fivebeans.client('localhost', 11300),
		connected			= false,

		listeningForReplies	= false,
		replyTTR			= 1000,

		defaultTube 		= 'work',
		replyTube 			= os.hostname() + '-' + process.pid + '-reply';

		callbacks 			= {};


	priv.generateId = uuid;

	priv.parsePayload = function(payload) {
		return JSON.parse(payload.toString());
	}

	priv.stringifyData = function(data) {
		return JSON.stringify(data);
	}

	priv.connect = function(callback) {
		if (connected) {
			callback();
		}
		else {
			async.parallel([
				function(callback) {
					workClient
						.on('connect', 	callback)
						.on('error', 	callback)
						.on('close', function() {
							connected = false;	
						})
						.connect();
				},
				function(callback) {
					replyClient
						.on('connect', 	callback)
						.on('error', 	callback)
						.on('close', function() {
							connected = false;	
						})
						.connect();
				}
			], function(err) {
				if (err) {
					connected = false;
				}
				else {
					connected = true;
					callback();
				}
			});

		}
	}

	priv.registerReplyCallback = function(callback) {
		var id = priv.generateId()
		callbacks[id] = callback;

		return id;
	}

	priv.executeReplyCallback = function(id, payload) {
		var callback = callbacks[id];
		if (id && callback) {
			callback(payload);
		}
	}

	priv.listenForReplies = function() {
		if (!listeningForReplies) {
			replyClient.watch(replyTube, priv.reserveReply);
			listeningForReplies = true;
		}
	}

	priv.reserveReply = function() {
		priv.connect(function() {
			replyClient.reserve(function(err, jobid, payload) {
				replyClient.destroy(jobid, function(err) {
					var data = priv.parsePayload(payload);

					priv.executeReplyCallback(data.callbackId, data.payload);
					priv.reserveReply();
				});
			});
		});
	}

	priv.reply = function(replyTo, callbackId, payload, successCallback) {
		successCallback = successCallback || function() {};

		var data = {
			tube:   	replyTo,
			callbackId: callbackId,
			payload: 	payload
		}

		replyClient.use(data.tube, function(err, tubename) {
			replyClient.put(0, 0, replyTTR, priv.stringifyData(data), successCallback);
		})
	}

	publ.put = function(tube, payload, successCallback, replyCallback, priority, delay, ttr) {
		tube 		= tube || defaultTube
		priority	= priority || 0;
		delay		= delay || 0;
		ttr 		= ttr || 0;

		priv.connect(function() {
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
				workClient.put(
					priority, 
					delay, 
					ttr, 
					priv.stringifyData(data), 
					successCallback
				);
			});
		});
	}


	publ.reserve = function(tube, successCallback) {
		tube = tube || defaultTube;
		successCallback = successCallback || function() {};

		priv.connect(function() {
			workClient.watch(tube, function(err, numwatched) {
				workClient.reserve(function(err, jobid, payload) {
					var data = priv.parsePayload(payload);

					successCallback(
						data.payload, 
						priv.generateFinishedCallback(jobid, payload)
					);
				});
			});
		});
	}


	priv.generateFinishedCallback = function(jobid, payload) {
		return function(replyPayload, successCallback) { 
			successCallback = successCallback || function() {};

			workClient.destroy(jobid, function(err) {
				var data = priv.parsePayload(payload);

				if (data.replyTo) {
					priv.reply(
						data.replyTo, 
						data.callbackId, 
						replyPayload, 
						successCallback
					)
				}
				else {
					successCallback();
				}
			});
		}
	}
}