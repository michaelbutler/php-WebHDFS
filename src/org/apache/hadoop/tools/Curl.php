<?php

namespace org\apache\hadoop\tools;

class Curl
{
    private $debug;
    private $lastRequestContentResult;
    private $lastRequestInfoResult;
    /**
     * @var array
     * curl options
     */
    private $options;

    public function __construct($debug = false)
    {
        $this->debug = $debug;
    }

    /**
     *
     * @param $localPath string local file path to save
     */
    public function setOption($key, $value)
    {
        $this->options[$key] = $value;
    }

    public function cleanLastRequest()
    {
        unset($this->lastRequestInfoResult);
        unset($this->lastRequestContentResult);
        gc_collect_cycles();

        return $this;
    }

    public function getWithRedirect($url)
    {
        return $this->get($url, array(CURLOPT_FOLLOWLOCATION => true));
    }

    public function get($url, $options = array())
    {
        $options[CURLOPT_URL] = $url;
        $options[CURLOPT_RETURNTRANSFER] = true;

        return $this->_exec($options);
    }

    public function putLocation($url)
    {
        return $this->_findRedirectUrl($url, array(CURLOPT_PUT => true));
    }

    public function postLocation($url)
    {
        return $this->_findRedirectUrl($url, array(CURLOPT_POST => true));
    }

    private function _findRedirectUrl($url, $options)
    {
        $options[CURLOPT_URL] = $url;
        $options[CURLOPT_HEADER] = true;
        $options[CURLOPT_RETURNTRANSFER] = true;
        $header = $this->_exec($options);
        $matches = array();
        if (preg_match('/Location:(.*?)\n/', $header, $matches)) {
            $redirectUrl = trim($matches[1]);

            return $redirectUrl;
        }

        return null;
    }

    public function putFile($url, $filename)
    {
        $options[CURLOPT_URL] = $url;
        $options[CURLOPT_PUT] = true;
        $handle = fopen($filename, "r");
        $options[CURLOPT_INFILE] = $handle;
        $options[CURLOPT_INFILESIZE] = filesize($filename);

        $info = $this->_exec($options, true);

        return ('201' == $info['http_code']);
    }

    public function putData($url, $data, $contentType = 'application/json')
    {
        $options[CURLOPT_URL] = $url;
        // $options[CURLOPT_PUT] = true;
        $options[CURLOPT_CUSTOMREQUEST] = 'PUT';
        $options[CURLOPT_POSTFIELDS] = $data;
        /*
        $options[CURLOPT_HTTPHEADER] = array(
            'Content-Length: '.strlen($data)
        );
        */
        $info = $this->_exec($options, true);

        return ('201' == $info['http_code']);
    }

    public function postString($url, $string)
    {
        $options[CURLOPT_URL] = $url;
        $options[CURLOPT_POST] = true;
        $options[CURLOPT_POSTFIELDS] = $string;

        $info = $this->_exec($options, true);

        return ('200' == $info['http_code']);
    }

    public function put($url)
    {
        $options = array();
        $options[CURLOPT_URL] = $url;
        $options[CURLOPT_PUT] = true;
        $options[CURLOPT_RETURNTRANSFER] = true;
        $options[CURLOPT_INFILESIZE] = 0;

        return $this->_exec($options);
    }

    public function post($url)
    {
        $options = array();
        $options[CURLOPT_URL] = $url;
        $options[CURLOPT_POST] = true;
        $options[CURLOPT_RETURNTRANSFER] = true;

        return $this->_exec($options);
    }

    public function delete($url)
    {
        $options = array();
        $options[CURLOPT_URL] = $url;
        $options[CURLOPT_CUSTOMREQUEST] = "DELETE";
        $options[CURLOPT_RETURNTRANSFER] = true;

        return $this->_exec($options);
    }

    public function getLastRequestContentResult($cleanLastRequest = false)
    {
        $r = $this->lastRequestContentResult;
        if ($cleanLastRequest) {
            $this->cleanLastRequest();
        }

        return $r;
    }

    public function getLastRequestInfoResult($cleanLastRequest = false)
    {
        $r = $this->lastRequestInfoResult;
        if ($cleanLastRequest) {
            $this->cleanLastRequest();
        }

        return $r;
    }

    public function validateLastRequest($cleanLastRequestIfValid = false)
    {
        $http_code = $this->getLastRequestInfoResult()['http_code'];
        if ($http_code >= 400 && $http_code <= 500) {
            return false;
        }
        if ($cleanLastRequestIfValid) {
            $this->cleanLastRequest();
        }

        return true;
    }

    private function _exec($options, $returnInfo = false)
    {
        $ch = curl_init();
        if ($this->debug === true) {
            $options[CURLOPT_VERBOSE] = true;
        }
        if (!isset($options[CURLOPT_RETURNTRANSFER])) {
            $options[CURLOPT_RETURNTRANSFER] = true;
        }
        if (!isset($options[CURLOPT_HTTPHEADER])) {
            $options[CURLOPT_HTTPHEADER] = array('Expect: ');
        } else {
            $options[CURLOPT_HTTPHEADER] = array_merge($options[CURLOPT_HTTPHEADER], array('Expect: '));
        }
        if (isset($this->options['local_file_handler'])) {
            $fp = $this->options['local_file_handler'];
            flock($fp, LOCK_EX);
            $options[CURLOPT_WRITEFUNCTION] = function ($ch, $string) use ($fp) {
                $length = fwrite($fp, $string);

                return $length;
            };
            fflush($fp);
        }
        // auto add content-length header
        $has_content_length_header = false;
        foreach ($options[CURLOPT_HTTPHEADER] as $header) {
            if (stripos($header, 'content-length:') === 0) {
                $has_content_length_header = true;
                break;
            }
        }
        if (!$has_content_length_header && !isset($options[CURLOPT_INFILE]) && !isset($options[CURLOPT_INFILESIZE])) {
            if (isset($options[CURLOPT_POSTFIELDS])) {
                // only for string content
                if (is_string($options[CURLOPT_POSTFIELDS]) && strpos($options[CURLOPT_POSTFIELDS], '@') !== 0) {
                    $options[CURLOPT_HTTPHEADER] = array_merge(
                        $options[CURLOPT_HTTPHEADER],
                        ['Content-Length: '.strlen($options[CURLOPT_POSTFIELDS])]
                    );
                }
            } else {
                $options[CURLOPT_HTTPHEADER] = array_merge($options[CURLOPT_HTTPHEADER], ['Content-Length: 0']);
            }
        }

        curl_setopt_array($ch, $options);
        // force clean memory before getting more data
        $this->cleanLastRequest();
        $result = curl_exec($ch);
        $this->lastRequestContentResult = $result;
        $this->lastRequestInfoResult = curl_getinfo($ch);
        if ($returnInfo) {
            $result = $this->lastRequestInfoResult;
        }
        if (isset($this->options['local_file_handler'])) {
            fclose($this->options['local_file_handler']);
        }
        curl_close($ch);

        return $result;
    }

}
