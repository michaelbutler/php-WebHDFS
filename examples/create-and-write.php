<?php
/**
 *
 *
 * @author Sebastian Lagemann <sebastian@iqu.com>
 */

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
var_dump($hdfs->create($arguments->target_path, $arguments->source_path));

?>