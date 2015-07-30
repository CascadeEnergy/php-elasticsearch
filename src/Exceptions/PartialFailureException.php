<?php

namespace CascadeEnergy\ElasticSearch\Exceptions;

class PartialFailureException extends \Exception
{
    private $errorList;

    public function __construct($message, array $errorList = [], $code = 0, $previous = null)
    {
        parent::__construct($message, $code, $previous);

        $this->errorList = $errorList;
    }

    public function getErrorList()
    {
        return $this->errorList;
    }
}
