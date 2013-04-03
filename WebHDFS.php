<?php

require_once __DIR__ . '/Curl.php';

class WebHDFS {

	public function __construct($host, $port, $user) {
		$this->host = $host;
		$this->port = $port;
		$this->user = $user;
	}

	// File and Directory Operations

	public function create($path, $filename) {
		if (!file_exists($filename)) {
			return false;
		}

		$url = $this->_buildUrl($path, array('op'=>'CREATE'));
		$redirectUrl = Curl::putLocation($url);
		return Curl::putFile($redirectUrl, $filename);
	}

	public function append($path, $string, $bufferSize='') {
		$url = $this->_buildUrl($path, array('op'=>'APPEND', 'buffersize'=>$bufferSize));
		$redirectUrl = Curl::postLocation($url);
		return Curl::postString($redirectUrl, $string);
	}

	public function concat($path, $sources) {
		$url = $this->_buildUrl($path, array('op'=>'CONCAT', 'sources'=>$sources));
		return Curl::post($url);
	}

	public function open($path, $offset='', $length='', $bufferSize='') {
		$url = $this->_buildUrl($path, array('op'=>'OPEN', 'offset'=>$offset, 'length'=>$length, 'buffersize'=>$bufferSize));
		return Curl::getWithRedirect($url);
	}

	public function mkdirs($path, $permission='') {
		$url = $this->_buildUrl($path, array('op'=>'MKDIRS', 'permission'=>$permission));
		return Curl::put($url);
	}

	public function createSymLink($path, $destination, $createParent='') {
		$url = $this->_buildUrl($destination, array('op'=>'CREATESYMLINK', 'destination'=>$path, 'createParent'=>$createParent));
		return Curl::put($url);
	}

	public function rename($path, $destination) {
		$url = $this->_buildUrl($path, array('op'=>'RENAME', 'destination'=>$destination));
		return Curl::put($url);
	}

	public function delete($path, $recursive='') {
		$url = $this->_buildUrl($path, array('op'=>'DELETE', 'recursive'=>$recursive));
		return Curl::delete($url);
	}

	public function getFileStatus($path) {
		$url = $this->_buildUrl($path, array('op'=>'GETFILESTATUS'));
		return Curl::get($url);
	}

	public function listStatus($path) {
		$url = $this->_buildUrl($path, array('op'=>'LISTSTATUS'));
		return Curl::get($url);
	}

	// Other File System Operations

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

	public function setPermission($path, $permission) {
		$url = $this->_buildUrl($path, array('op'=>'SETPERMISSION', 'permission'=>$permission));
		return Curl::put($url);
	}

	public function setOwner($path, $owner='', $group='') {
		$url = $this->_buildUrl($path, array('op'=>'SETOWNER', 'owner'=>$owner, 'group'=>$group));
		return Curl::put($url);
	}

	public function setReplication($path, $replication) {
		$url = $this->_buildUrl($path, array('op'=>'SETREPLICATION', 'replication'=>$replication));
		return Curl::put($url);
	}

	public function setTimes($path, $modificationTime='', $accessTime='') {
		$url = $this->_buildUrl($path, array('op'=>'SETTIMES', 'modificationtime'=>$modificationTime, 'accesstime'=>$accessTime));
		return Curl::put($url);
	}

	private function _buildUrl($path, $query_data) {
		if ($path[0] == '/') {
			$path = substr($path, 1);
		}

		$query_data['user.name'] = $this->user;
		return 'http://' . $this->host . ':' . $this->port . '/webhdfs/v1/' . $path . '?' . http_build_query(array_filter($query_data));
	}

}
