<?php
/**
 *
 *
 * @author Sebastian Lagemann <sebastian@iqu.com>
 */

namespace org\apache\hadoop;

/**
 * Class WebHDFS_Exception
 * exception class for errors while using WebHDFS
 *
 * @package org\apache\hadoop
 */
class WebHDFS_Exception extends \Exception
{
    const FILE_ALREADY_EXISTS = 1;
    const FILE_NOT_FOUND = 2;
    const PERMISSION_DENIED = 3;
}
