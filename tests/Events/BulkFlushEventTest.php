<?php

namespace CascadeEnergy\Tests\ElasticSearch\Events;

use CascadeEnergy\ElasticSearch\Events\BulkFlushEvent;

class BulkFlushEventTests extends \PHPUnit_Framework_TestCase
{
    /** @var BulkFlushEvent */
    private $event;

    public function setUp()
    {
        $this->event = new BulkFlushEvent('index', 'type', 42);
    }

    public function testItShouldExposeTheTargetIndex()
    {
        $this->assertEquals('index', $this->event->getIndex());
    }

    public function testItShouldExposeTheTargetType()
    {
        $this->assertEquals('type', $this->event->getType());
    }

    public function testItshouldExposeTheNumberOfRowsFlushed()
    {
        $this->assertEquals(42, $this->event->getRowCount());
    }
}
