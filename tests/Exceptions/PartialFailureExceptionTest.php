<?php

namespace CascadeEnergy\Tests\ElasticSearch\Exceptions;

use CascadeEnergy\ElasticSearch\Exceptions\PartialFailureException;

class PartialFailureExceptionTest extends \PHPUnit_Framework_TestCase
{
    /** @var PartialFailureException */
    private $exception;

    /** @var \Exception */
    private $previous;

    public function setUp()
    {
        $this->previous = new \Exception();
        $this->exception = new PartialFailureException('message', ['foo', 'bar', 'baz'], 42, $this->previous);
    }

    public function testItShouldPassThroughTheStandardExceptionInformation()
    {
        $this->assertEquals('message', $this->exception->getMessage());
        $this->assertEquals(42, $this->exception->getCode());
        $this->assertSame($this->previous, $this->exception->getPrevious());
    }

    public function testItShouldExposeAListOfFailedOperations()
    {
        $this->assertEquals(['foo', 'bar', 'baz'], $this->exception->getErrorList());
    }
}
