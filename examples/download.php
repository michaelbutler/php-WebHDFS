<?php
/**
 * @author pengxiang.dai <ehovel@126.com>
 */

//$fp = fopen('/tmp/t.pack','a');
//var_dump($fp);
//$a = function () use ($fp) {
//    var_dump($fp);
//    //$fp = $this->options['local_file_handler'];
//    //$fp = fopen('/tmp/t.pack','a');
//    $length = fwrite($fp, "ddd");
//    //var_dump(111);exit;
//    return $length;
//};
//$a();exit;


require_once __DIR__.'/argv-parser.php';
require_once __DIR__.'/../vendor/autoload.php';

$arguments = getArguments();
$hdfs = new \org\apache\hadoop\WebHDFS(
	$arguments->namenode_host,
	$arguments->namenode_port,
	$arguments->namenode_user,
	$arguments->namenode_rpc_host,
	$arguments->namenode_rpc_port,
	$arguments->debug === 'true'
);
var_dump($hdfs->open($arguments->source_path,'','','', $arguments->target_path));

?>