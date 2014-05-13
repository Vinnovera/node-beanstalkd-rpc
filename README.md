Node.js beanstalkd RPC
=======================
A slim and simple beanstalkd based RPC client for node, based on fivebeans.
https://github.com/ceejbot/fivebeans

## Produce jobs
`client.put(tube, payload, successCallback function(err, jobid) {}, replyCallback function(payload) {}, priority, delay, ttr)`
* `successCallback` fires when the payload has been sent.
* `replyCallback` fires when a reply payload is received.

```javascript
var bRPC	= require('node-beanstalkd-rpc'),
	client	= new bRPC('localhost', 11300);

client.put(
	'workQueue',
	'Go work!',
	function(err) {
		console.log('job sent');
	},
	function(err, finished, payload) {
		console.log('Got reply ' + payload + ' and job is ' + (finished ? 'finished' : 'not finished'));
	}
);
``` 

## Consume jobs
`client.reserve(tube, successCallback function(payload, reply) {})`
* `successCallback` fires when a payload has been reserved from the queue. 
* `reply` is a function used to report progress or mark the job as being finished. With an optional payload as argument.

```javascript
var bRPC	= require('node-beanstalkd-rpc'),
	client	= new bRPC('localhost', 11300);

client.reserve('workQueue', function(err, payload, reply) {
	console.log('job received containing ' + payload);

	reply(false, '50% done!', function() {
		console.log('progress successfully sent');
	});

	reply(true, 'Work done!', function() {
		console.log('done successfully sent');
	});
})
``` 
