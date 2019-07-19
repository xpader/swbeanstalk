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

	public function use($tube)
	{
		$this->send(sprintf("use %s", $tube));
		$ret = $this->recv();
		if ($ret['status'] == 'USING' && $ret['meta'][0] == $tube) {
			return true;
		} else {
			$this->setError($ret['status'], "Use tube $tube failed.");
			return false;
		}
	}

	public function reserve($timeout=null)
	{
		if (isset($timeout)) {
			$this->send(sprintf('reserve-with-timeout %d', $timeout));
		} else {
			$this->send('reserve');
		}
		
		$res = $this->recv();

		if ($res['status'] == 'RESERVED') {
			list($id, $bytes) = $res['meta'];
			return [
				'id' => $id,
				'body' => substr($res['body'], 0, $bytes)
			];
		} else {
			$this->setError($res['status']);
			return false;
		}
	}

	public function delete($id)
	{
		return $this->sendv(sprintf('delete %d', $id), 'DELETED');
	}

	public function release($id)
	{
		return $this->sendv(sprintf('release %d', $id), 'RELEASED');
	}

	public function bury($id)
	{
		return $this->sendv(sprintf('bury %d', $id), 'BURIED');
	}

	public function touch($id)
	{
		return $this->sendv(sprintf('touch %d', $id), 'TOUCHED');
	}

	public function watch($tube)
	{
		$this->send(sprintf('watch %s', $tube));
		$res = $this->recv();

		if ($res['status'] == 'WATCHING') {
			return $res['meta'][0];
		} else {
			$this->setError($res['status']);
			return false;
		}
	}

	public function ignore($tube)
	{
		return $this->sendv(sprintf('ignore %s', $tube), 'WATCHING');
	}

	public function peek($id)
	{
		$this->send(sprintf('peek %d', $id));
		return $this->peekRead();
	}

	public function peekReady()
	{
		$this->send('peek-ready');
		return $this->peekRead();
	}

	public function peekDelayed()
	{
		$this->send('peek-delayed');
		return $this->peekRead();
	}

	public function peekBuried()
	{
		$this->send('peek-buried');
		return $this->peekRead();
	}

	protected function peekRead()
	{
		$res = $this->recv();

		if ($res['status'] == 'FOUND') {
			list($id, $bytes) = $res['meta'];
			return [
				'id' => $id,
				'body' => substr($res['body'], 0, $bytes)
			];
		} else {
			$this->setError($res['status']);
			return false;
		}
	}

	public function kick($bound)
	{
		$this->send(sprintf('kick %d', $bound));
		$res = $this->recv();

		if ($res['status'] == 'KICKED') {
			return $res['meta'][0];
		} else {
			$this->setError($res['status']);
			return false;
		}
	}

	public function kickJob($id)
	{
		return $this->sendv(sprintf('kick-job %d', $id), 'KICKED');
	}

	public function statsJob($id)
	{
		$this->send(sprintf('stats-job %d', $id));
		return $this->statsRead();
	}

	public function statsTube($tube)
	{
		$this->send(sprintf('stats-tube %s', $tube));
		return $this->statsRead();
	}

	public function stats()
	{
		$this->send('stats');
		return $this->statsRead();
	}

	public function listTubes()
	{
		$this->send('list-tubes');
		return $this->statsRead();
	}

	public function listTubeUsed()
	{
		$this->send('list-tube-used');
		$res = $this->recv();
		if ($res['status'] == 'USING') {
			return $res['meta'][0];
		} else {
			$this->setError($res['status']);
			return false;
		}
	}

	public function listTubesWatched()
	{
		$this->send('list-tubes-watched');
		return $this->statsRead();
	}

	protected function statsRead()
	{
		$res = $this->recv();

		if ($res['status'] == 'OK') {
			list($bytes) = $res['meta'];
			$body = trim($res['body']);
		
			$data = array_slice(explode("\n", $body), 1);
			$result = [];

			foreach ($data as $row) {
				if ($row{0} == '-') {
					$value = substr($row, 2);
					$key = null;
				} else {
					$pos = strpos($row, ':');
					$key = substr($row, 0, $pos);
					$value = substr($row, $pos+2);
				}
				if (is_numeric($value)) {
					$value = (int)$value == $value ? (int)$value : (float)$value;
				}
				isset($key) ? $result[$key] = $value : array_push($result, $value);
			}
			return $result;
		} else {
			$this->setError($res['status']);
			return false;
		}
	}

	public function pauseTube($tube, $delay)
	{
		return $this->sendv(sprintf('pause-tube %s %d', $tube, $delay));
	}

	protected function sendv($cmd, $status)
	{
		$this->send($cmd);
		$res = $this->recv();

		if ($res['status'] != $status) {
			$this->setError($res['status']);
			return false;
		}

		return true;
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
			$this->wrap($cmd);
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
			$this->wrap($recv);
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

	protected function wrap($output)
	{
		echo "\r\n<<-----\r\n$output\r\n----->>\r\n";
	}

}

