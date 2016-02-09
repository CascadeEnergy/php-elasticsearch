<?php

namespace CascadeEnergy\Tests\ElasticSearch\Iterators;

use CascadeEnergy\ElasticSearch\Iterators\SearchResponseIterator;

class SearchResponseIteratorTest extends \PHPUnit_Framework_TestCase
{
    /** @var \PHPUnit_Framework_MockObject_MockObject */
    private $client;

    /** @var SearchResponseIterator */
    private $iterator;

    private $params;

    public function setUp()
    {
        $this->client = $this->getMockBuilder('Elasticsearch\Client')->disableOriginalConstructor()->getMock();
        $this->params = ['scroll' => '1m'];
    }

    public function testTheScrollTimeoutShouldBeAdjustable()
    {
        /** @noinspection PhpParamsInspection */
        $this->iterator = new SearchResponseIterator($this->client, $this->params);

        $this->iterator->setScrollTimeout('5m');

        $this->assertAttributeEquals('5m', 'scrollTtl', $this->iterator);
    }

    public function testItShouldDoNothingIfTheScrollIdIsClearedWhenThereIsNoScrollId()
    {
        /** @noinspection PhpParamsInspection */
        $this->iterator = new SearchResponseIterator($this->client, $this->params);

        $this->client->expects($this->never())->method('clearScroll');
        $this->iterator->clearScroll();
    }

    public function testItShouldBePossibleToClearTheScrollId()
    {
        $this->client->method('search')->with($this->params)->willReturn(['_scroll_id' => 'foo-scroll-id']);
        $this->client->expects($this->once())->method('clearScroll')->with(['scroll_id' => 'foo-scroll-id']);

        /** @noinspection PhpParamsInspection */
        $this->iterator = new SearchResponseIterator($this->client, $this->params);

        $this->iterator->rewind();
        $this->iterator->clearScroll();

        // Calling this a second time ensures that the scroll ID is reset to NULL, and that we don't try to clear it
        // twice (we'd see a second call the `clearScroll` if we did).
        $this->iterator->clearScroll();
    }

    public function testRewindingShouldReRunTheSearch()
    {
        $this->client->expects($this->once())->method('clearScroll');
        $this->client
            ->method('search')
            ->with($this->params)
            ->willReturnOnConsecutiveCalls(
                ['_scroll_id' => 'foo-scroll-id'],
                ['_scroll_id' => 'bar-scroll-id']
            );

        /** @noinspection PhpParamsInspection */
        $this->iterator = new SearchResponseIterator($this->client, $this->params);

        $this->iterator->rewind();
        $this->iterator->rewind();
        $this->assertAttributeEquals('bar-scroll-id', 'scrollId', $this->iterator);
    }

    public function testNextShouldAdvanceTheScrolledResults()
    {
        $this->client->method('search')->with($this->params)->willReturn(['_scroll_id' => 'foo-scroll-id']);

        $this->client->expects($this->once())->method('scroll')->with([
            'scroll_id' => 'foo-scroll-id',
            'scroll' => '1m'
        ]);

        /** @noinspection PhpParamsInspection */
        $this->iterator = new SearchResponseIterator($this->client, $this->params);

        $this->iterator->rewind();
        $this->iterator->next();
    }

    public function testTheIteratorIsValidIfWeAreAtTheBeginningOfTheResults()
    {
        /** @noinspection PhpParamsInspection */
        $this->iterator = new SearchResponseIterator($this->client, $this->params);

        $this->iterator->rewind();
        $this->assertTrue($this->iterator->valid());
    }

    public function testTheIteratorIsValidIfWeReceivedAtLeastOneHit()
    {
        $this->client->method('search')->with($this->params)->willReturn([
            'hits' => ['hits' => ['foo' => 'bar']],
            '_scroll_id' => 'foo-scroll-id'
        ]);

        /** @noinspection PhpParamsInspection */
        $this->iterator = new SearchResponseIterator($this->client, $this->params);

        $this->iterator->rewind();

        $this->assertTrue($this->iterator->valid());
    }

    public function testTheIteratorIsInvalidIfWeReceivedNoHits()
    {
        $this->client->expects($this->once())->method('search')->willReturn([
            'hits' => ['hits' => []],
            '_scroll_id' => 'foo-scroll-id'
        ]);

        /** @noinspection PhpParamsInspection */
        $this->iterator = new SearchResponseIterator($this->client, $this->params);

        $this->iterator->rewind();

        $this->assertTrue($this->iterator->valid());
    }

    public function testTheIteratorShouldReturnTheCurrentPageOfResults()
    {
        $results = ['foo' => 'bar', '_scroll_id' => 'foo-scroll-id'];
        $this->client->expects($this->once())->method('search')->willReturn($results);

        /** @noinspection PhpParamsInspection */
        $this->iterator = new SearchResponseIterator($this->client, $this->params);

        $this->iterator->rewind();
        $this->assertEquals($results, $this->iterator->current());
    }

    public function testTheCurrentKeyShouldIncrementWithEachCallToNext()
    {
        $results = ['foo' => 'bar', '_scroll_id' => 'foo-scroll-id'];
        $this->client->expects($this->once())->method('search')->willReturn($results);

        /** @noinspection PhpParamsInspection */
        $this->iterator = new SearchResponseIterator($this->client, $this->params);

        $this->iterator->rewind();
        $this->assertEquals(0, $this->iterator->key());
        $this->iterator->next();
        $this->assertEquals(1, $this->iterator->key());
    }
}
