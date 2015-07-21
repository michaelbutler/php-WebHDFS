<?php
/**
 *
 *
 * @author Sebastian Lagemann <sebastian@iqu.com>
 */

function getArguments($options = array(
	'namenode-host' => array('required' => true),
	'namenode-user' => array('default' => 'root'),
	'namenode-port' => array('default' => 50070),
	'namenode-rpc-host' => array('required' => true),
	'namenode-rpc-port' => array('default' => 8022),
	'target-path' => array('required' => true),
	'source-path' => array('required' => true),
	'debug' => array('default' => 'false')
)) {
	$longOpts = array();
	foreach ($options AS $optName => $opt) {
		if (isset($opt['required']) && $opt['required'] === true) {
			$longOpts[] = $optName . ':';
		} else {
			$longOpts[] = $optName . '::';
		}
	}
	$optionResult = getopt("", $longOpts);
	$values = array();
	foreach ($options AS $optKey => $option) {
		$oOptKey = preg_replace('/-/', '_', $optKey);
		if (!isset($optionResult[$optKey]) && isset($option['default'])) {
			$values[$oOptKey] = $option['default'];
		} elseif (!isset($optionResult[$optKey]) && isset($option['required']) && $option['required'] === true) {
			throw new Exception(sprintf('missing argument %1$s', $optKey));
		} else {
			$values[$oOptKey] = $optionResult[$optKey];
		}
	}
	return (object)$values;
}

?>