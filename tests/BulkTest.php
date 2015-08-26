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
        $this->bulk = new Bulk($this->client, 'index', 'type', 3);
    }

    public function testItShouldAllowItemsToBeAddedToTheBulkOperation()
    {
        $this->bulk->addItem('idFoo', 'foo');
        $this->bulk->addItem('idBar', 'bar', 'update');

        $this->assertAttributeEquals(
            [
                ['index' => ['_id' => 'idFoo']],
                'foo',
                ['update' => ['_id' => 'idBar']],
                'bar'
            ],
            'itemList',
            $this->bulk
        );

        $this->assertEquals(2, $this->bulk->getItemCount());
    }

    public function testItShouldFlushAutomaticallyWhenTooManyItemsHaveBeenAdded()
    {
        $this->client->expects($this->once())->method('bulk');

        $this->bulk->addItem('idFoo', 'foo');
        $this->bulk->addItem('idBar', 'bar');
        $this->bulk->addItem('idBaz', 'baz');

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
