<?php

class WebHDFS {

	public function __construct($host, $port, $user) {
		$this->host = $host;
		$this->port = $port;
		$this->user = $user;
	}

	public function open($path) {
		$query_data = array('user.name'=>$this->user, 'op'=>'OPEN');
		$url = 'http://' . $this->host . ':' . $this->port . '/webhdfs/v1' . $path . '?' . http_build_query($query_data);

		$options = array(
				CURLOPT_URL => $url,
				CURLOPT_RETURNTRANSFER => true,
				CURLOPT_FOLLOWLOCATION => true,
		);

		$ch = curl_init();
		curl_setopt_array($ch, $options);
		$result = curl_exec($ch);
		curl_close($ch);

		return $result;
	}
}
