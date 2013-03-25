<?php

class Curl {

	public static function getWithRedirect($url) {
		return self::get($url, array(CURLOPT_FOLLOWLOCATION => true));
	}

	public static function get($url, $options=array()) {
		$options[CURLOPT_URL] = $url;
		$options[CURLOPT_RETURNTRANSFER] = true;
		return self::_exec($options);
	}

	public static function putLocation($url) {
		return self::_findRedirectUrl($url, array(CURLOPT_PUT=>true));
	}

	public static function postLocation($url) {
		return self::_findRedirectUrl($url, array(CURLOPT_POST=>true));
	}

	private static function _findRedirectUrl($url, $options) {
		$options[CURLOPT_URL] = $url;
		$info = self::_exec($options, true);
		return $info['redirect_url'];
	}

	public static function putFile($url, $filename) {
		$options[CURLOPT_URL] = $url;
		$options[CURLOPT_PUT] = true;
		$handle = fopen($filename, "r");
		$options[CURLOPT_INFILE] = $handle;
		$options[CURLOPT_INFILESIZE] = filesize($filename);

		$info = self::_exec($options, true);

		return ('201' == $info['http_code']);
	}

	public static function postFile($url, $filename) {
		$options[CURLOPT_URL] = $url;
		$options[CURLOPT_POST] = true;
		$options[CURLOPT_POSTFIELDS] = file_get_contents($filename);

		$info = self::_exec($options, true);

		return ('200' == $info['http_code']);
	}

	public static function put($url) {
		$options = array();
		$options[CURLOPT_URL] = $url;
		$options[CURLOPT_PUT] = true;
		$options[CURLOPT_RETURNTRANSFER] = true;

		return self::_exec($options);
	}

	public static function delete($url) {
		$options = array();
		$options[CURLOPT_URL] = $url;
		$options[CURLOPT_CUSTOMREQUEST] = "DELETE";
		$options[CURLOPT_RETURNTRANSFER] = true;

		return self::_exec($options);
	}

	private static function _exec($options, $returnInfo=false) {
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
