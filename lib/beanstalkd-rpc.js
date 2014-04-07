module.exports = function(host, port) {
	host = host || 'localhost';
	port = port || 11300;
	
	var publ		= this,
		priv		= {},

		os			= require('os'),
		uuid 		= require('node-uuid').v4,
		async 		= require('async'),
		fivebeans 	= require('fivebeans'),

		workClient			= new fivebeans.client(host, port),
		replyClient			= new fivebeans.client(host, port),
		connected			= false,

		listeningForReplies	= false,
		replyTTR			= 1000,

		defaultTube 		= 'work',
		replyTube 			= os.hostname() + '-' + process.pid + '-reply',

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
			return callback(null);
		}

		async.parallel([
			function(callback) {
				workClient
					.on('connect',  callback)
					.on('error',    callback)
					.on('close',    function() {
						connected = false;
					})
					.connect();
			},
			function(callback) {
				replyClient
					.on('connect',  callback)
					.on('error',    callback)
					.on('close',    function() {
						connected = false;
					})
					.connect();
			}
		], function(err) {
			if (err) {
				connected = false;
				return callback(err);
			}

			connected = true;
			callback(null);
		});
	}

	priv.registerReplyCallback = function(callback) {
		var id = priv.generateId()
		callbacks[id] = callback;

		return id;
	}

	priv.executeReplyCallback = function(err, id, payload) {
		var callback = callbacks[id];
		if (id && callback) {
			if (err) return callback(err);

			callback(null, payload);
		}
	}

	priv.listenForReplies = function() {
		if (!listeningForReplies) {
			replyClient.watch(replyTube, priv.reserveReply);
			listeningForReplies = true;
		}
	}

	priv.reserveReply = function(err) {
		priv.connect(function(err) {
			replyClient.reserve(function(err, jobid, payload) {
				var data = priv.parsePayload(payload);

				if (err) return priv.executeReplyCallback(err, data.callbackId);

				replyClient.destroy(jobid, function(err) {
					if (err) return priv.executeReplyCallback(err, data.callbackId);

					priv.executeReplyCallback(null, data.callbackId, data.payload);
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
			if (err) return successCallback(err);

			replyClient.put(0, 0, replyTTR, priv.stringifyData(data), successCallback);
		})
	}

	publ.put = function(tube, payload, successCallback, replyCallback, priority, delay, ttr) {
		tube 		= tube || defaultTube
		priority	= priority || 0;
		delay		= delay || 0;
		ttr 		= ttr || 0;

		priv.connect(function(err) {
			if (err) return successCallback(err);

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
				if (err) return successCallback(err);

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

		priv.connect(function(err) {
			if (err) return successCallback(err);

			workClient.watch(tube, function(err, numwatched) {
				if (err) return successCallback(err);

				workClient.reserve(function(err, jobid, payload) {
					if (err) return successCallback(err);

					var data = priv.parsePayload(payload);

					successCallback(
						null,
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
				if (err) return successCallback(err);

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
					successCallback(null);
				}
			});
		}
	}
}