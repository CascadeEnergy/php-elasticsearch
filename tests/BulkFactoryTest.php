<?php

namespace CascadeEnergy\Tests\ElasticSearch;

use CascadeEnergy\ElasticSearch\BulkFactory;

class BulkFactoryTest extends \PHPUnit_Framework_TestCase
{
    /** @var BulkFactory */
    private $bulkFactory;

    /** @var \PHPUnit_Framework_MockObject_MockObject */
    private $client;

    public function setUp()
    {
        $this->client = $this->getMockBuilder('Elasticsearch\Client')->disableOriginalConstructor()->getMock();

        /** @noinspection PhpParamsInspection */
        $this->bulkFactory = new BulkFactory($this->client);
    }

    public function testItShouldCreateBulkObjects()
    {
        $bulk = $this->bulkFactory->createBulk('index', 'type');

        $this->assertInstanceOf('CascadeEnergy\ElasticSearch\Bulk', $bulk);
        $this->assertAttributeEquals('index', 'index', $bulk);
        $this->assertAttributeEquals('type', 'type', $bulk);
        $this->assertAttributeEquals(null, 'eventDispatcher', $bulk);
    }

    public function testItShouldConfigureBulkOperationsWithAnEventDispatcherIfOneIsAvailable()
    {
        $eventDispatcher = $this->getMock('Symfony\Component\EventDispatcher\EventDispatcherInterface');

        /** @noinspection PhpParamsInspection */
        $this->bulkFactory->setEventDispatcher($eventDispatcher);
        $bulk = $this->bulkFactory->createBulk('index', 'type');

        $this->assertAttributeSame($eventDispatcher, 'eventDispatcher', $bulk);
    }
}
