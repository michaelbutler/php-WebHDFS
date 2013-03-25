<?php

require_once __DIR__ . '/Curl.php';

class WebHDFS {

	public function __construct($host, $port, $user) {
		$this->host = $host;
		$this->port = $port;
		$this->user = $user;
	}

	public function create($path, $filename) {
		if (!file_exists($filename)) {
			return false;
		}

		$url = $this->_buildUrl($path, array('op'=>'CREATE'));
		$redirectUrl = Curl::putLocation($url);
		return Curl::putFile($redirectUrl, $filename);
	}

	public function append($path, $filename) {
		if (!file_exists($filename)) {
			return false;
		}

		$url = $this->_buildUrl($path, array('op'=>'APPEND'));
		$redirectUrl = Curl::postLocation($url);

		return Curl::postFile($redirectUrl, $filename);
	}

	public function open($path) {
		$url = $this->_buildUrl($path, array('op'=>'OPEN'));
		return Curl::getWithRedirect($url);
	}

	public function mkdirs($path) {
		$url = $this->_buildUrl($path, array('op'=>'MKDIRS'));
		return Curl::put($url);
	}

	public function rename($path, $newPath) {
		$url = $this->_buildUrl($path, array('op'=>'RENAME', 'destination'=>$newPath));
		return Curl::put($url);
	}

	public function getFileStatus($path) {
		$url = $this->_buildUrl($path, array('op'=>'GETFILESTATUS'));
		return Curl::get($url);
	}

	public function listStatus($path) {
		$url = $this->_buildUrl($path, array('op'=>'LISTSTATUS'));
		return Curl::get($url);
	}

	public function getContentSummary($path) {
		$url = $this->_buildUrl($path, array('op'=>'GETCONTENTSUMMARY'));
		return Curl::get($url);
	}

	public function getFileChecksum($path) {
		$url = $this->_buildUrl($path, array('op'=>'GETFILECHECKSUM'));
		return Curl::getWithRedirect($url);
	}

	public function getHomeDirectory() {
		$url = $this->_buildUrl('', array('op'=>'GETHOMEDIRECTORY'));
		return Curl::get($url);
	}

	private function _buildUrl($path, $query_data) {
		$query_data['user.name'] = $this->user;
		return 'http://' . $this->host . ':' . $this->port . '/webhdfs/v1/' . $path . '?' . http_build_query($query_data);
	}

}
