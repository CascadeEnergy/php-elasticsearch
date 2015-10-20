<?php

namespace CascadeEnergy\ElasticSearch\Exceptions;

/**
 * This exception is raised when at least some of the items in a Bulk operation fail.
 */
class PartialFailureException extends \Exception
{
    private $errorList;

    /**
     * @param string $message
     * @param array $errorList An array of error messages from the bulk operation
     * @param int $code
     * @param null $previous
     */
    public function __construct($message, array $errorList = [], $code = 0, $previous = null)
    {
        parent::__construct($message, $code, $previous);

        $this->errorList = $errorList;
    }

    /**
     * @return array An array of error messages from the bulk operation
     */
    public function getErrorList()
    {
        return $this->errorList;
    }
}
