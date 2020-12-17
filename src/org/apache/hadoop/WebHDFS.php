<?php

namespace org\apache\hadoop;

use org\apache\hadoop\tools\Curl;

class WebHDFS
{
    private $host;
    private $port;
    private $user;
    private $namenode_rpc_host;
    private $namenode_rpc_port;
    private $debug;
    private $use_ssl = false;
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

    public function create(
        $path,
        $filename,
        $overwrite = false,
        $blocksize = null,
        $replication = null,
        $permission = null,
        $buffersize = null
    ) {
        if (!file_exists($filename)) {
            return false;
        }

        $options = array(
            'op' => 'CREATE',
            'overwrite' => $overwrite,
            'blocksize' => $blocksize,
            'replication' => $replication,
            'permission' => $permission,
            'buffersize' => $buffersize,
        );
        $url = $this->_buildUrl($path, $options);
        $redirectUrl = $this->curl->putLocation($url);
        $result = $this->curl->putFile($redirectUrl, $filename);
        if ($result !== true) {
            throw $this->getResponseErrorException($this->curl->getLastRequestContentResult());
        }

        return $result;
    }

    public function createWithData(
        $path,
        $data,
        $overwrite = false,
        $blockSize = null,
        $replication = null,
        $permission = null,
        $bufferSize = null
    ) {
        $options = array(
            'op' => 'CREATE',
            'overwrite' => $overwrite,
            'blocksize' => $blockSize,
            'replication' => $replication,
            'permission' => $permission,
            'buffersize' => $bufferSize,
        );
        $url = $this->_buildUrl($path, $options);
        $redirectUrl = $this->curl->putLocation($url);
        $result = false;
        if ($redirectUrl) {
            $result = $this->curl->putData($redirectUrl, $data);
        }
        if ($result !== true) {
            throw $this->getResponseErrorException($this->curl->getLastRequestContentResult());
        }

        return $result;
    }

    public function append($path, $string, $bufferSize = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'APPEND', 'buffersize' => $bufferSize));
        $redirectUrl = $this->curl->postLocation($url);

        return $this->curl->postString($redirectUrl, $string);
    }

    public function concat($path, $sources)
    {
        $url = $this->_buildUrl($path, array('op' => 'CONCAT', 'sources' => $sources));

        return $this->curl->post($url);
    }

    public function open($path, $offset = '', $length = '', $bufferSize = '',$localFile = '')
    {
        $url = $this->_buildUrl($path,
            array('op' => 'OPEN', 'offset' => $offset, 'length' => $length, 'buffersize' => $bufferSize));
        if ($localFile) {
            $fp = fopen($localFile,'w');
            if (!$fp) {
                return new WebHDFS_Exception('local file open failed', WebHDFS_Exception::LOCAL_FILE_OPEN_ERR);
            }
            $this->curl->setOption('local_file_handler',$fp);
        }

        $result = $this->curl->getWithRedirect($url);
        if ($this->curl->validateLastRequest(true)) {
            return $result;
        }
        throw $this->getResponseErrorException($this->curl->getLastRequestContentResult(true));
    }

    public function mkdirs($path, $permission = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'MKDIRS', 'permission' => $permission));

        return $this->curl->put($url);
    }

    public function createSymLink($path, $destination, $createParent = '')
    {
        $url = $this->_buildUrl($destination,
            array('op' => 'CREATESYMLINK', 'destination' => $path, 'createParent' => $createParent));

        return $this->curl->put($url);
    }

    public function rename($path, $destination)
    {
        $url = $this->_buildUrl($path, array('op' => 'RENAME', 'destination' => $destination));

        return $this->curl->put($url);
    }

    public function delete($path, $recursive = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'DELETE', 'recursive' => $recursive));

        return $this->curl->delete($url);
    }

    public function truncate($path, $newLength = 0)
    {
        $url = $this->_buildUrl($path, array('op' => 'TRUNCATE', 'newlength' => $newLength));

        return $this->curl->post($url);
    }

    public function getFileStatus($path)
    {
        $url = $this->_buildUrl($path, array('op' => 'GETFILESTATUS'));
        $r = $this->curl->get($url);
        $this->curl->cleanLastRequest();

        return $r;
    }

    public function listStatus($path)
    {
        return $this->_listStatus($path, true);
    }

    private function _listStatus($path, $cleanLastRequest = false)
    {
        $url = $this->_buildUrl($path, array('op' => 'LISTSTATUS'));
        if ($result = $this->curl->get($url)) {
            if ($cleanLastRequest) {
                $this->curl->cleanLastRequest();
            }
            $result = json_decode($result);
            if (!is_null($result)) {
                return $result;
            } else {
                throw $this->getResponseErrorException($this->curl->getLastRequestContentResult($cleanLastRequest));
            }
        } else {
            throw $this->getResponseErrorException($this->curl->getLastRequestContentResult($cleanLastRequest));
        }
    }

    /**
     * @param string $path
     * @param bool $recursive
     * @param bool $includeFileMetaData
     * @param int $maxAmountOfFiles
     * @return array
     * @throws WebHDFS_Exception
     */
    public function listFiles($path, $recursive = false, $includeFileMetaData = false, $maxAmountOfFiles = 0)
    {
        $result = array();
        $listStatusResult = $this->_listStatus($path);
        $r = $this->curl->getLastRequestContentResult(true);

        if (isset($listStatusResult->FileStatuses->FileStatus)) {
            foreach ($listStatusResult->FileStatuses->FileStatus AS $fileEntity) {
                switch ($fileEntity->type) {
                    case 'DIRECTORY':
                        if ($recursive === true) {
                            $result = array_merge(
                                $result,
                                $this->listFiles(
                                    $this->concatPath([$path, $fileEntity->pathSuffix]),
                                    true,
                                    $includeFileMetaData,
                                    $maxAmountOfFiles - sizeof($result)
                                )
                            );
                        }
                        break;
                    default:
                        if ($includeFileMetaData === true) {
                            $fileEntity->path = $this->concatPath([$path, $fileEntity->pathSuffix]);
                            $result[] = $fileEntity;
                        } else {
                            $result[] = $this->concatPath([$path, $fileEntity->pathSuffix]);
                        }
                }
                // recursion will be interrupted since we subtract the amount of the current result set from the maxAmountOfFiles amount with calling the next recursion
                if ($maxAmountOfFiles !== 0 && sizeof($result) >= $maxAmountOfFiles) {
                    break;
                }
            }
        } else {
            throw $this->getResponseErrorException($r);
        }

        return $result;
    }

    public function listDirectories($path, $recursive = false, $includeFileMetaData = false)
    {
        $result = array();
        $listStatusResult = $this->_listStatus($path);
        $r = $this->curl->getLastRequestContentResult(true);

        if (isset($listStatusResult->FileStatuses->FileStatus)) {
            foreach ($listStatusResult->FileStatuses->FileStatus AS $fileEntity) {
                switch ($fileEntity->type) {
                    case 'DIRECTORY':
                        if ($includeFileMetaData === true) {
                            $fileEntity->path = $this->concatPath([$path, $fileEntity->pathSuffix]);
                            $result[] = $fileEntity;
                        } else {
                            $result[] = $this->concatPath([$path, $fileEntity->pathSuffix]);
                        }
                        if ($recursive === true) {
                            $result = array_merge($result,
                                $this->listDirectories(
                                    $this->concatPath([$path, $fileEntity->pathSuffix]),
                                    $recursive,
                                    $includeFileMetaData
                                )
                            );
                        }
                        break;
                }
            }
        } else {
            throw $this->getResponseErrorException($r);
        }

        return $result;
    }

    // Other File System Operations

    public function getContentSummary($path)
    {
        $url = $this->_buildUrl($path, array('op' => 'GETCONTENTSUMMARY'));
        $rawResult = $this->curl->get($url);
        $resultDecoded = json_decode($rawResult);
        if (isset($resultDecoded->ContentSummary)) {
            $result = $resultDecoded->ContentSummary;
        } else {
            throw $this->getResponseErrorException($this->curl->getLastRequestContentResult(true));
        }

        return $result;
    }

    public function getFileChecksum($path)
    {
        $url = $this->_buildUrl($path, array('op' => 'GETFILECHECKSUM'));

        return $this->curl->getWithRedirect($url);
    }

    public function getHomeDirectory()
    {
        $url = $this->_buildUrl('', array('op' => 'GETHOMEDIRECTORY'));

        return $this->curl->get($url);
    }

    public function getTrashRoot()
    {
        $url = $this->_buildUrl('', array('op' => 'GETTRASHROOT'));

        return $this->curl->get($url);
    }

    public function setPermission($path, $permission)
    {
        $url = $this->_buildUrl($path, array('op' => 'SETPERMISSION', 'permission' => $permission));

        return $this->curl->put($url);
    }

    public function setOwner($path, $owner = '', $group = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'SETOWNER', 'owner' => $owner, 'group' => $group));

        return $this->curl->put($url);
    }

    public function setReplication($path, $replication)
    {
        $url = $this->_buildUrl($path, array('op' => 'SETREPLICATION', 'replication' => $replication));

        return $this->curl->put($url);
    }

    public function modifyAclEntries($path, $aclSpec = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'MODIFYACLENTRIES', 'aclspec' => $aclSpec));

        return $this->curl->put($url);
    }

    public function removeAclEntries($path, $aclSpec = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'REMOVEACLENTRIES', 'aclspec' => $aclSpec));

        return $this->curl->put($url);
    }

    public function removeDefaultAcl($path)
    {
        $url = $this->_buildUrl($path, array('op' => 'REMOVEDEFAULTACL'));

        return $this->curl->put($url);
    }

    public function removeAcl($path)
    {
        $url = $this->_buildUrl($path, array('op' => 'REMOVEACL'));

        return $this->curl->put($url);
    }

    public function setAcl($path, $aclSpec = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'SETACL', 'aclspec' => $aclSpec));

        return $this->curl->put($url);
    }

    public function getAclStatus($path)
    {
        $url = $this->_buildUrl($path, array('op' => 'GETACLSTATUS'));

        return $this->curl->get($url);
    }

    public function checkAccess($path, $fsaction = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'CHECKACCESS', 'fsaction' => $fsaction));

        return $this->curl->get($url);
    }

    public function setTimes($path, $modificationTime = '', $accessTime = '')
    {
        $url = $this->_buildUrl($path,
            array('op' => 'SETTIMES', 'modificationtime' => $modificationTime, 'accesstime' => $accessTime));

        return $this->curl->put($url);
    }

    public function useSsl($use_ssl)
    {
        $this->use_ssl = $use_ssl;

        return $this;
    }

    private function _buildUrl($path, $query_data)
    {
        if (strlen($path) && $path[0] == '/') {
            $path = substr($path, 1);
        }

        if (!isset($query_data['user.name'])) {
            $query_data['user.name'] = $this->user;
        }
        // it is required to specify the namenode rpc address in, at least, write requests
        if (!isset($query_data['namenoderpcaddress'])) {
            $query_data['namenoderpcaddress'] = $this->namenode_rpc_host.':'.$this->namenode_rpc_port;
        }
        $protocol = 'http'.($this->use_ssl ? 's' : '');

        return $protocol.'://'.$this->host.':'.$this->port.'/webhdfs/v1/'.$path.'?'.http_build_query(array_filter($query_data));
    }

    /**
     * returns a generated exception for given response data
     *
     * @param $responseData
     * @return WebHDFS_Exception
     */
    private function getResponseErrorException($responseData)
    {
        $data = json_decode($responseData);

        $exceptionCode = 0;
        $exceptionMessage = 'invalid/unknown response/exception: '.$responseData;
        if (!is_null($data)) {
            if (
                isset($data->RemoteException->exception) &&
                isset($data->RemoteException->javaClassName) &&
                isset($data->RemoteException->message)
            ) {
                $exceptionMessage = $data->RemoteException->exception.' in '.$data->RemoteException->javaClassName."\n".
                    $data->RemoteException->message;
                switch ($data->RemoteException->javaClassName) {
                    case 'org.apache.hadoop.fs.FileAlreadyExistsException':
                        $exceptionCode = WebHDFS_Exception::FILE_ALREADY_EXISTS;
                        break;
                    case 'java.io.FileNotFoundException':
                        $exceptionCode = WebHDFS_Exception::FILE_NOT_FOUND;
                        break;
                    case 'org.apache.hadoop.security.AccessControlException':
                        if (preg_match('/Permission denied/i', $data->RemoteException->message)) {
                            $exceptionCode = WebHDFS_Exception::PERMISSION_DENIED;
                        }
                        break;
                };
            }
        }

        return new WebHDFS_Exception($exceptionMessage, $exceptionCode);
    }

    private function concatPath(array $paths) {
        $result = '';
        foreach ($paths as $path) {
            if (!$result || preg_match('/.+\/$/', $result)) {
                $result .= $path;
                continue;
            }

            $result .= '/' . $path;
        }
        return $this->removeMultiSlashFromPath($result);
    }

    private function removeMultiSlashFromPath($path) {
        return preg_replace('/(\/)\/+/', '$1', $path);
    }

}
