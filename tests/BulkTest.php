<?php

namespace CascadeEnergy\Tests\ElasticSearch;

use CascadeEnergy\ElasticSearch\Bulk;

class BulkTest extends \PHPUnit_Framework_TestCase
{
    /** @var Bulk */
    private $bulk;

    /** @var \PHPUnit_Framework_MockObject_MockObject */
    private $client;

    public function setUp()
    {
        $this->client = $this->getMockBuilder('Elasticsearch\Client')->disableOriginalConstructor()->getMock();

        /** @noinspection PhpParamsInspection */
        $this->bulk = new Bulk($this->client, 'index', 'type', 5);
    }

    public function testItShouldAllowItemsToBeAddedToTheBulkOperation()
    {
        $this->bulk->addItem('idFoo', 'foo');
        $this->bulk->addItem('idBar', 'bar', 'update');
        $this->bulk->addItem('idQux', 'qux', 'index', 'indexQux');
        $this->bulk->addItem('idQuux', 'quux', 'update', 'indexQuux', 'typeQuux');

        $this->assertAttributeEquals(
            [
                ['index' => ['_id' => 'idFoo']],
                'foo',
                ['update' => ['_id' => 'idBar']],
                'bar',
                ['index' => ['_id' => 'idQux', '_index' => 'indexQux']],
                'qux',
                ['update' => ['_id' => 'idQuux', '_index' => 'indexQuux', '_type' => 'typeQuux']],
                'quux'
            ],
            'itemList',
            $this->bulk
        );

        $this->assertEquals(4, $this->bulk->getItemCount());
    }

    public function testItShouldFlushAutomaticallyWhenTooManyItemsHaveBeenAdded()
    {
        $bulkResponse = new \stdClass();
        $bulkResponse->items = ['foo', 'bar', 'baz', 'qux', 'quux'];
        $bulkResponse->errors = false;

        $this->client->expects($this->once())->method('bulk')->willReturn($bulkResponse);

        $this->bulk->addItem('idFoo', 'foo');
        $this->bulk->addItem('idBar', 'bar');
        $this->bulk->addItem('idBaz', 'baz');
        $this->bulk->addItem('idQux', 'qux');
        $this->bulk->addItem('idQuux', 'quux');

        $this->assertEquals(0, $this->bulk->getItemCount());
    }

    public function testItShouldFlushWhenBeginAndEndAreCalled()
    {
        $bulkResponse = new \stdClass();
        $bulkResponse->items = ['foo'];
        $bulkResponse->errors = false;

        $this->client->expects($this->exactly(2))->method('bulk')->willReturn($bulkResponse);

        $this->bulk->addItem('idFoo', 'foo');
        $this->bulk->begin();

        $this->bulk->addItem('idFoo', 'foo');
        $this->bulk->end();
    }

    public function testItShouldDoNothingIfFlushIsCalledWhenTheItemListIsEmpty()
    {
        $this->client->expects($this->never())->method('bulk');
        $this->bulk->flush();
    }

    public function testItShouldSendAllTheItemsInTheListToTheBulkEndpoint()
    {
        $bulkResponse = new \stdClass();
        $bulkResponse->items = ['foo', 'bar'];
        $bulkResponse->errors = false;

        $expectedParameters = [
            'index' => 'index',
            'type' => 'type',
            'body' => [
                ['index' => ['_id' => 'idFoo']],
                'foo',
                ['index' => ['_id' => 'idBar']],
                'bar'
            ]
        ];

        $this->client->expects($this->once())->method('bulk')->with($expectedParameters)->willReturn($bulkResponse);

        $this->bulk->addItem('idFoo', 'foo');
        $this->bulk->addItem('idBar', 'bar');
        $this->bulk->flush();
    }

    public function testItShouldRaiseAnExceptionIfSomeItemsAreNotInTheSuccessfulItemsList()
    {
        $result = new \stdClass();
        $result->items = ['foo'];
        $result->errors = false;

        $this->client->expects($this->once())->method('bulk')->willReturn($result);

        $this->setExpectedException('CascadeEnergy\ElasticSearch\Exceptions\PartialFailureException');

        $this->bulk->addItem('idFoo', 'foo');
        $this->bulk->addItem('idBar', 'bar');
        $this->bulk->flush();
    }

    public function testItShouldRaiseAnExceptionIfTheErrorListIsNotEmpty()
    {
        $result = new \stdClass();
        $result->items = ['foo'];
        $result->errors = true;

        $this->client->expects($this->once())->method('bulk')->willReturn($result);

        $this->setExpectedException('CascadeEnergy\ElasticSearch\Exceptions\PartialFailureException');

        $this->bulk->addItem('idFoo', 'foo');
        $this->bulk->flush();
    }

    public function testItShouldEmptyTheItemListOnClear()
    {
        $this->bulk->addItem('idfoo','foo');
        $this->assertEquals($this->bulk->getItemCount(), 1);
        $this->bulk->clear();
        $this->assertEquals($this->bulk->getItemCount(), 0);
    }
}
