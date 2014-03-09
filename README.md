Node.js beanstalkd RPC
=======================
A slim and simple beanstalkd based RPC client for node, based on fivebeans.
https://github.com/ceejbot/fivebeans

## Produce jobs
`client.put(tube, payload, successCallback function(err, jobid) {}, replyCallback function(payload) {}, priority, delay, ttr)`
* `successCallback` fires when the payload has been sent.
* `replyCallback` fires when a reply payload is received.

```javascript
var bRPC	= require('./beanstalkd-rpc.js'),
	client	= new bRPC(localhost, 11300);

client.put(
	'work', 
	'Go work!', 
	function() { 
		console.log('job sent'); 
	}, 
	function(payload) { 
		console.log('job is finished and returned ' + payload); 
	}
);
``` 

## Consume jobs
`client.reserve(tube, successCallback function(payload, finishedCallback) {})`
* `successCallback` fires when a payload has been reserved from the queue. 
* `finishedCallback` is a function used to report the job as being finished with an optional payload as first argument.

```javascript
var bRPC	= require('./beanstalkd-rpc.js'),
	client	= new bRPC(localhost, 11300);

client.reserve('work', function(payload, finishedCallback) {
	console.log('job received containing ' + payload);

	finishedCallback('Work done!', function() { 
		console.log('reply successfully sent');
	});
})
``` 
