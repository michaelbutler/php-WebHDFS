<?php

class WebHDFS {

	public function __construct($host, $port, $user) {
		$this->host = $host;
		$this->port = $port;
		$this->user = $user;
	}

	public function open($path) {
		return $this->_getWithRedirect('OPEN', $path);
	}

	public function getFileStatus($path) {
		return $this->_get('GETFILESTATUS', $path);
	}

	public function listStatus($path) {
		return $this->_get('LISTSTATUS', $path);
	}

	public function getContentSummary($path) {
		return $this->_get('GETCONTENTSUMMARY', $path);
	}

	public function getFileChecksum($path) {
		return $this->_getWithRedirect('GETFILECHECKSUM', $path);
	}

	public function getHomeDirectory() {
		return $this->_get('GETHOMEDIRECTORY');
	}

	public function create($path, $filename) {
		if (!file_exists($filename)) {
			return false;
		}

		$url = $this->_putLocation('CREATE', $path);
		return $this->_putToUrl($url, $filename);
	}

	private function _getWithRedirect($operation, $path) {
		return $this->_get($operation, $path, array(CURLOPT_FOLLOWLOCATION => true));
	}

	private function _get($operation, $path='', $options=array()) {
		$options[CURLOPT_URL] = $this->_buildUrl($path, array('op'=>$operation));
		$options[CURLOPT_RETURNTRANSFER] = true;
		return $this->_exec($options);
	}

	private function _putLocation($operation, $path) {
		$options[CURLOPT_URL] = $this->_buildUrl($path, array('op'=>$operation));
		$options[CURLOPT_PUT] = true;

		$info = $this->_exec($options, true);

		return $info['redirect_url'];
	}

	private function _putToUrl($url, $filename) {
		$options[CURLOPT_URL] = $url;
		$options[CURLOPT_PUT] = true;
		$handle = fopen($filename, "r");
		$options[CURLOPT_INFILE] = $handle;
		$options[CURLOPT_INFILESIZE] = filesize($filename);

		$info = $this->_exec($options, true);

		return ('201' == $info['http_code']);
	}

	// 	private function _putWithRedirect($operation, $path, $filename) {
	// 		return $this->_put($operation, $path, $filename, array(CURLOPT_FOLLOWLOCATION => true));
	// 	}

	// 	private function _put($operation, $path, $filename, $options=array()) {
	// 		// 		$options[CURLOPT_VERBOSE] = true;
	// 		$options[CURLOPT_URL] = $this->_buildUrl($path, array('op'=>$operation));
	// 		$options[CURLOPT_PUT] = true;
	// 		$options[CURLOPT_HEADER] = true;
	// 		$options[CURLOPT_RETURNTRANSFER] = true;
	// 		$handle = fopen($filename, "r");
	// 		$options[CURLOPT_INFILE] = $handle;
	// 		$options[CURLOPT_INFILESIZE] = filesize($filename);

	// 		return $this->_exec($options);
	// 	}

	private function _buildUrl($path, $query_data) {
		$query_data['user.name'] = $this->user;
		return 'http://' . $this->host . ':' . $this->port . '/webhdfs/v1/' . $path . '?' . http_build_query($query_data);
	}

	private function _exec($options, $returnInfo=false) {
		$ch = curl_init();
		curl_setopt_array($ch, $options);
		$result = curl_exec($ch);
		if ($returnInfo) {
			$result = curl_getinfo($ch);
		}
		curl_close($ch);

		return $result;
	}

}
