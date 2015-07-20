<?php

namespace org\apache\hadoop;

use org\apache\hadoop\tools\Curl;

class WebHDFS {
	private $host;
	private $port;
	private $user;
	private $namenode_rpc_host;
	private $namenode_rpc_port;
	private $debug;
	/**
	 * @var Curl
	 */
	private $curl;

	public function __construct(
		$host,
		$port,
		$user,
		$namenodeRpcHost,
		$namenodeRpcPort,
		$debug
	) {
		$this->host = $host;
		$this->port = $port;
		$this->user = $user;
		$this->namenode_rpc_host = $namenodeRpcHost;
		$this->namenode_rpc_port = $namenodeRpcPort;
		$this->debug = $debug;
		$this->curl = new Curl($this->debug);
	}

	// File and Directory Operations

	public function create($path, $filename) {
		if (!file_exists($filename)) {
			return false;
		}

		$url = $this->_buildUrl($path, array('op'=>'CREATE'));
		$redirectUrl = $this->curl->putLocation($url);
		$result = $this->curl->putFile($redirectUrl, $filename);
		if($result !== true) {
			throw $this->getResponseErrorException($this->curl->getLastRequestContentResult());
		}
		return $result;
	}
	public function createWithData($path, $data) {
		$url = $this->_buildUrl($path, array('op' => 'CREATE'));
		$redirectUrl = $this->curl->putLocation($url);
		$result = $this->curl->putData($redirectUrl, $data);
		if($result !== true) {
			throw $this->getResponseErrorException($this->curl->getLastRequestContentResult());
		}
		return $result;
	}

	public function append($path, $string, $bufferSize='') {
		$url = $this->_buildUrl($path, array('op'=>'APPEND', 'buffersize'=>$bufferSize));
		$redirectUrl = $this->curl->postLocation($url);
		return $this->curl->postString($redirectUrl, $string);
	}

	public function concat($path, $sources) {
		$url = $this->_buildUrl($path, array('op'=>'CONCAT', 'sources'=>$sources));
		return $this->curl->post($url);
	}

	public function open($path, $offset='', $length='', $bufferSize='') {
		$url = $this->_buildUrl($path, array('op'=>'OPEN', 'offset'=>$offset, 'length'=>$length, 'buffersize'=>$bufferSize));
		return $this->curl->getWithRedirect($url);
	}

	public function mkdirs($path, $permission='') {
		$url = $this->_buildUrl($path, array('op'=>'MKDIRS', 'permission'=>$permission));
		return $this->curl->put($url);
	}

	public function createSymLink($path, $destination, $createParent='') {
		$url = $this->_buildUrl($destination, array('op'=>'CREATESYMLINK', 'destination'=>$path, 'createParent'=>$createParent));
		return $this->curl->put($url);
	}

	public function rename($path, $destination) {
		$url = $this->_buildUrl($path, array('op'=>'RENAME', 'destination'=>$destination));
		return $this->curl->put($url);
	}

	public function delete($path, $recursive='') {
		$url = $this->_buildUrl($path, array('op'=>'DELETE', 'recursive'=>$recursive));
		return $this->curl->delete($url);
	}

	public function getFileStatus($path) {
		$url = $this->_buildUrl($path, array('op'=>'GETFILESTATUS'));
		return $this->curl->get($url);
	}

	public function listStatus($path) {
		$url = $this->_buildUrl($path, array('op'=>'LISTSTATUS'));
		return $this->curl->get($url);
	}

	// Other File System Operations

	public function getContentSummary($path) {
		$url = $this->_buildUrl($path, array('op'=>'GETCONTENTSUMMARY'));
		return $this->curl->get($url);
	}

	public function getFileChecksum($path) {
		$url = $this->_buildUrl($path, array('op'=>'GETFILECHECKSUM'));
		return $this->curl->getWithRedirect($url);
	}

	public function getHomeDirectory() {
		$url = $this->_buildUrl('', array('op'=>'GETHOMEDIRECTORY'));
		return $this->curl->get($url);
	}

	public function setPermission($path, $permission) {
		$url = $this->_buildUrl($path, array('op'=>'SETPERMISSION', 'permission'=>$permission));
		return $this->curl->put($url);
	}

	public function setOwner($path, $owner='', $group='') {
		$url = $this->_buildUrl($path, array('op'=>'SETOWNER', 'owner'=>$owner, 'group'=>$group));
		return $this->curl->put($url);
	}

	public function setReplication($path, $replication) {
		$url = $this->_buildUrl($path, array('op'=>'SETREPLICATION', 'replication'=>$replication));
		return $this->curl->put($url);
	}

	public function setTimes($path, $modificationTime='', $accessTime='') {
		$url = $this->_buildUrl($path, array('op'=>'SETTIMES', 'modificationtime'=>$modificationTime, 'accesstime'=>$accessTime));
		return $this->curl->put($url);
	}

	private function _buildUrl($path, $query_data) {
		if (strlen($path) && $path[0] == '/') {
			$path = substr($path, 1);
		}

		if(!isset($query_data['user.name'])) {
			$query_data['user.name'] = $this->user;
		}
		// it is required to specify the namenode rpc address in, at least, write requests
		if(!isset($query_data['namenoderpcaddress'])) {
			$query_data['namenoderpcaddress'] = $this->namenode_rpc_host.':'.$this->namenode_rpc_port;
		}
		return 'http://' . $this->host . ':' . $this->port . '/webhdfs/v1/' . $path . '?' . http_build_query(array_filter($query_data));
	}

	/**
	 * returns a generated exception for given response data
	 *
	 * @param $responseData
	 * @return WebHDFS_Exception
	 */
	private function getResponseErrorException($responseData) {
		$data = json_decode($responseData);

		$exceptionCode = 0;
		$exceptionMessage = 'invalid/unknown response/exception: '.$responseData;
		if(!is_null($data)) {
			if(
				isset($data->RemoteException->exception) &&
				isset($data->RemoteException->javaClassName) &&
				isset($data->RemoteException->message)
			) {
				$exceptionMessage = $data->RemoteException->exception . ' in ' . $data->RemoteException->javaClassName . "\n" .
					$data->RemoteException->message;
				switch($data->RemoteException->javaClassName) {
					case 'org.apache.hadoop.fs.FileAlreadyExistsException':
						$exceptionCode = WebHDFS_Exception::FILE_ALREADY_EXISTS;
						break;

				}
				;
			}
		}
		return new WebHDFS_Exception($exceptionMessage, $exceptionCode);
	}
}

?>