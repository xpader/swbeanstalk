<?php

use Swoole\Coroutine;
use pader\swbeanstalk\Swbeanstalk;

require_once __DIR__.'/../vendor/autoload.php';


function coJobConsumer($id) {
	echo "Create consumer: $id.\n";

	$workerId = $id;

	$client = getConnection();
	$client->watch('test');
	$client->ignore('default');

	echo $workerId." connected\r\n";

	while (true) {
		$res = $client->reserve();

		echo $workerId." get {$res['id']} {$res['body']}\r\n";

		if ($client->delete($res['id'])) {
			echo "Deleted\r\n";
		}
	}
}

function coJobProducer() {
	echo "Create producer.\n";

	$client = getConnection();
	echo "producer connected\r\n";

	$client->useTube('test');

	while (true) {
		$ts = time();
		$ret = $client->put($ts);
		echo "Put job id: $ret\r\n";
		Co::sleep(0.2);
	}
}

function getConnection() {
	$client = new Swbeanstalk('172.16.0.181', 11300, 1);
	if (!$client->connect()) {
		throw new \ErrorException('Connect to beanstalkd failed.');
	}
	return $client;
}

$scheduler = new Coroutine\Scheduler;

$workerNum = 4;

if ($workerNum > 0) {
	for ($i=0; $i<$workerNum; $i++) {
		$scheduler->add('coJobConsumer', $i+1);
	}
}

$scheduler->add('coJobProducer');

// $scheduler->add(function() {
// 	$client = getConnection();
// 	$stats = $client->statsTube('test');
// 	print_r($stats);
// });

$scheduler->start();


