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
        $this->client = $this->getMock('Elasticsearch\Client');

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
        $this->client->expects($this->once())->method('bulk');

        $this->bulk->addItem('idFoo', 'foo');
        $this->bulk->addItem('idBar', 'bar');
        $this->bulk->addItem('idBaz', 'baz');
        $this->bulk->addItem('idQux', 'qux');
        $this->bulk->addItem('idQuux', 'quux');

        $this->assertEquals(0, $this->bulk->getItemCount());
    }

    public function testItShouldFlushWhenBeginAndEndAreCalled()
    {
        $this->client->expects($this->exactly(2))->method('bulk');

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

        $this->client->expects($this->once())->method('bulk')->with($expectedParameters);

        $this->bulk->addItem('idFoo', 'foo');
        $this->bulk->addItem('idBar', 'bar');
        $this->bulk->flush();
    }

    public function testItShouldRaiseAnExceptionIfSomeItemsFail()
    {
        $result = new \stdClass();
        $result->errors = ['qux', 'quux'];

        $this->client->expects($this->once())->method('bulk')->willReturn($result);

        $this->setExpectedException('CascadeEnergy\ElasticSearch\Exceptions\PartialFailureException');

        $this->bulk->addItem('idFoo', 'foo');
        $this->bulk->flush();
    }
}
