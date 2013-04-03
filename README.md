# php-WebHDFS

## Description

php-WebHDFS is a PHP client for [WebHDFS](http://hadoop.apache.org/docs/r2.0.3-alpha/hadoop-project-dist/hadoop-hdfs/WebHDFS.html).


## Dependencies
* [PHP](http://php.net/)
* [cURL](http://curl.haxx.se/)


## Usage

* [File and Directory Operations](#file-and-directory-operations)
  * [Create and Write to a File](#create-and-write-to-a-file)
  * [Append to a File](#append-to-a-file)
  * [Concat File(s)](#concat-files)
  * [Open and Read a File](#open-and-read-a-file)
  * [Make a Directory](#make-a-directory)
  * [Create a Symbolic Link](#create-a-symbolic-link)
  * [Rename a File/Directory](#rename-a-filedirectory)
  * [Delete a File/Directory](#delete-a-filedirectory)
  * [Status of a File/Directory](#status-of-a-filedirectory)
  * [List a Directory](#list-a-directory)
* [Other File System Operations](#other-file-system-operations)
  * [Get Content Summary of a Directory](#get-content-summary-of-a-directory)
  * [Get File Checksum](#get-file-checksum)
  * [Get Home Directory](#get-home-directory)
  * [Set Permission](#set-permission)
  * [Set Owner](#set-owner)
  * [Set Replication Factor](#set-replication-factor)
  * [Set Access or Modification Time](#set-access-or-modification-time)

### File and Directory Operations

#### Create and Write to a File
```php
hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$hdfs->create('user/hadoop-username/new-file.txt', 'local-file.txt');
```

#### Append to a File
```php
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$hdfs->append('user/hadoop-username/file-to-append-to.txt', 'local-file.txt');
```

#### Concat File(s)
```php
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$hdfs->concat('user/hadoop-username/concatenated-file.txt', '/test/file1,/test/file2,/test/file3');
```

#### Open and Read a File
```php
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$response = $hdfs->open('user/hadoop-username/file.txt');
```

#### Make a Directory
```php
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$hdfs->mkdirs('user/hadoop-username/new/directory/structure');
```

#### Create a Symbolic Link
```php
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$hdfs->createSymLink('user/hadoop-username/file.txt', '/user/hadoop-username/symlink-to-file.txt');
````

#### Rename a File/Directory
```php
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$hdfs->rename('user/hadoop-username/file.txt', '/user/hadoop-username/renamed-file.txt');
````

#### Delete a File/Directory
```php
hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$hdfs->delete('user/hadoop-username/file.txt');
```

#### Status of a File/Directory
```php
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$response = $hdfs->getFileStatus('user/hadoop-username/file.txt');
```

#### List a Directory
```php
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$response = $hdfs->listStatus('user/hadoop-username/');
```

### Other File System Operations

#### Get Content Summary of a Directory
```php
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$response = $hdfs->getContentSummary('user/hadoop-username/');
```

#### Get File Checksum
```php
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$response = $hdfs->getFileChecksum('user/hadoop-username/file.txt');
```

#### Get Home Directory
```php
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$response = $hdfs->getHomeDirectory();
```

#### Set Permission
```php
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$hdfs->setPermission('user/hadoop-username/file.txt', '777');
````

#### Set Owner
```php
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$hdfs->setOwner('user/hadoop-username/file.txt', 'other-user');
````

#### Set Replication Factor
```php
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$hdfs->setReplication('user/hadoop-username/file.txt', '2');
````

#### Set Access or Modification Time
```php
$hdfs = new WebHDFS('mynamenode.hadoop.com', '50070', 'hadoop-username');
$response = $hdfs->setTimes('user/hadoop-username/file.txt');
```