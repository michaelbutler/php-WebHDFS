<?php

namespace org\apache\hadoop\tools;

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
		$options[CURLOPT_HEADER] = true;
		$options[CURLINFO_EFFECTIVE_URL] = true;
		$options[CURLOPT_RETURNTRANSFER] = true;
		$header = self::_exec($options);
		$matches = array();
		preg_match('/Location:(.*?)\n/', $header, $matches);
		$redirectUrl = trim($matches[1]);
		return $redirectUrl;
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

	public static function postString($url, $string) {
		$options[CURLOPT_URL] = $url;
		$options[CURLOPT_POST] = true;
		$options[CURLOPT_POSTFIELDS] = $string;

		$info = self::_exec($options, true);

		return ('200' == $info['http_code']);
	}

	public static function put($url) {
		$options = array();
		$options[CURLOPT_URL] = $url;
		$options[CURLOPT_PUT] = true;
		$options[CURLOPT_RETURNTRANSFER] = true;
		$options[CURLOPT_INFILESIZE] = 0;

		return self::_exec($options);
	}

	public static function post($url) {
		$options = array();
		$options[CURLOPT_URL] = $url;
		$options[CURLOPT_POST] = true;
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
		$options[CURLOPT_VERBOSE] = true;
		$options[CURLOPT_HTTPHEADER] = array('Expect: ');
		curl_setopt_array($ch, $options);
		$result = curl_exec($ch);

		if ($returnInfo) {
			$result = curl_getinfo($ch);
		}

		curl_close($ch);
		return $result;
	}

}
