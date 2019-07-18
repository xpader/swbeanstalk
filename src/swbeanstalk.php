<?php

class SWBeanstalk {

	const DEFAULT_PRI = 60;
	const DEFAULT_TTR = 30;

	protected $config;
	protected $connection;
	protected $connected = false;
	protected $lastError = null;

	public $debug = false;

	public function __construct($host='127.0.0.1', $port=11300, $timeout=-1)
	{
		$this->config = compact('host', 'port', 'timeout');
	}

	public function connect()
	{
		if ($this->connection) {
			$this->disconnect();
		}

		$client = new \Swoole\Coroutine\Client(SWOOLE_SOCK_TCP);
		$this->connected = $client->connect($this->config['host'], $this->config['port'], $this->config['timeout']);

		if ($this->connected) {
			$this->connection = $client;
		} else {
			$client->close();
		}
	}

	public function put($data, $pri=self::DEFAULT_PRI, $delay=0, $ttr=self::DEFAULT_TTR)
	{
		$this->send(sprintf("put %d %d %d %d\r\n%s", $pri, $delay, $ttr, strlen($data), $data));
		$res = $this->recv();

		if ($res['status'] == 'INSERTED') {
			return $res['meta'][0];
		} else {
			$this->setError($res['status']);
			return false;
		}
	}

	public function reserve()
	{
		$this->send('reserve');
		$res = $this->recv();

		switch ($res['status']) {
			case 'RESERVED':
				list($id, $bytes) = $res['meta'];
				return [
					'id' => $id,
					'body' => substr($res['body'], 0, $bytes)
				];
				break;
		}
	}

	public function delete($id)
	{
		$this->send("delete $id");
		$res = $this->recv();

		if ($res['status'] == 'DELETED') {
			return true;
		} else {
			$this->setError($res['status']);
			return false;
		}
	}

	protected function send($cmd)
	{
		if (!$this->connected) {
			throw new \RuntimeException('No connecting found while writing data to socket.');
		}

		$cmd .= "\r\n";
		$len = strlen($cmd);
		$writeLen = $this->connection->send($cmd);

		if ($this->debug) {
			echo "\r\n<<-----\r\n$cmd\r\n----->>\r\n";
		}

		if ($writeLen != $len) {
			throw new \RuntimeException('Write data to socket failed.');
		}

		return $writeLen;
	}

	protected function recv()
	{
		if (!$this->connected) {
			throw new \RuntimeException('No connection found while reading data from socket.');
		}

		$recv = $this->connection->recv();
		$metaEnd = strpos($recv, "\r\n");
		$meta = explode(' ', substr($recv, 0, $metaEnd));
		$status = array_shift($meta);

		if ($this->debug) {
			echo "\r\n<<-----\r\n$recv\r\n----->>\r\n";
		}

		return [
			'status' => $status,
			'meta' => $meta,
			'body' => substr($recv, $metaEnd+2)
		];
	}

	public function disconnect()
	{
		if ($this->connected) {
			$this->send('quit');
			$this->connection->close();
			$this->connected = false;
		}

		if ($this->connection) {
			$this->connection = null;
		}
	}

	protected function setError($status, $msg='')
	{
		$this->lastError = compact('status', 'msg');
	}

	public function getError()
	{
		if ($this->lastError) {
			$error = $this->lastError;
			$this->lastError = null;
			return $error;
		}
		return null;;
	}

}

