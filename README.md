# php-WebHDFS

## Description

php-WebHDFS is a PHP client for [WebHDFS](http://hadoop.apache.org/docs/r2.0.3-alpha/hadoop-project-dist/hadoop-hdfs/WebHDFS.html).


## Dependencies
* [PHP](http://php.net/)
* [cURL](http://curl.haxx.se/)


## Usage

### File and Directory Operations

#### Create and Write to a File
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$hdfs->create('user/hadoop-username/new-file.txt', 'local-file.txt');

#### Append to a File
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$response = $hdfs->append('user/hadoop-username/file-to-append-to.txt', 'local-file.txt');

#### Concat File(s)
Not yet implemented.

#### Open and Read a File
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$response = $hdfs->open('user/hadoop-username/file.txt');

#### Make a Directory
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$response = $hdfs->mkdirs('user/hadoop-username/new/directory/structure');

#### Create a Symbolic Link
Not yet implemented.

#### Rename a File/Directory
Not yet implemented.

#### Delete a File/Directory
Not yet implemented.

#### Status of a File/Directory
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$response = $hdfs->getFileStatus('user/hadoop-username/file.txt');

#### List a Directory
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$response = $hdfs->listStatus('user/hadoop-username/');
