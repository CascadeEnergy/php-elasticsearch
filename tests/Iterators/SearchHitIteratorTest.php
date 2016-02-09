<?php

namespace CascadeEnergy\Tests\ElasticSearch\Iterators;

use CascadeEnergy\ElasticSearch\Iterators\SearchHitIterator;

class SearchHitIteratorTest extends \PHPUnit_Framework_TestCase
{
    /** @var SearchHitIterator */
    private $iterator;

    /** @var \PHPUnit_Framework_MockObject_MockObject */
    private $searchResponseIterator;

    public function setUp() {
        $this->searchResponseIterator = $this
            ->getMockBuilder('CascadeEnergy\ElasticSearch\Iterators\SearchResponseIterator')
            ->disableOriginalConstructor()
            ->getMock();

        /** @noinspection PhpParamsInspection */
        $this->iterator = new SearchHitIterator($this->searchResponseIterator);
    }

    public function testRewindingShouldExtractTheFirstHitFromTheFirstPageOfHits()
    {
        $currentPage = ['hits' => ['hits' => ['foo']]];

        $this->searchResponseIterator->expects($this->exactly(2))->method('valid')->willReturn(true);
        $this->searchResponseIterator->expects($this->exactly(2))->method('current')->willReturn($currentPage);

        $this->iterator->rewind();

        $this->assertAttributeEquals('foo', 'currentHitData', $this->iterator);
    }

    public function testIfTheFirstPageIsEmptyRewindingShouldRequestTheSecondPage()
    {
        $firstPage = ['hits' => ['hits' => []]];
        $secondPage = ['hits' => ['hits' => ['foo']]];

        $this->searchResponseIterator->expects($this->exactly(2))->method('valid')->willReturn(true);
        $this->searchResponseIterator
            ->expects($this->exactly(2))
            ->method('current')
            ->willReturnOnConsecutiveCalls($firstPage, $secondPage);

        $this->iterator->rewind();

        $this->assertAttributeEquals('foo', 'currentHitData', $this->iterator);
    }

    public function testIfTheFirstAndSecondPagesAreNotValidTheCurrentDataShouldBeSetToNull()
    {
        $this->searchResponseIterator
            ->expects($this->exactly(2))
            ->method('valid')
            ->willReturnOnConsecutiveCalls(false, false);
        $this->searchResponseIterator->expects($this->exactly(1))->method('current');

        $this->iterator->rewind();

        $this->assertAttributeEquals(null, 'currentHitData', $this->iterator);
    }

    public function testTheIteratorIsValidIfTheCurrentHitDataIsAnArray()
    {
        $this->searchResponseIterator->method('valid')->willReturn(true);
        $this->searchResponseIterator->method('current')->willReturn([
            'hits' => [
                'hits'=> [
                    ['foo' => 'bar']
                ]
            ]
        ]);

        $this->iterator->rewind();
        $this->assertTrue($this->iterator->valid());
    }

    public function testTheIteratorIsNotValidIfTheCurrentHitDataIsNotAnArray()
    {
        $this->searchResponseIterator->method('valid')->willReturn(true);
        $this->searchResponseIterator->method('current')->willReturn(['hits' => ['hits'=> ['foo']]]);

        $this->iterator->rewind();
        $this->assertFalse($this->iterator->valid());
    }

    public function testTheIteratorReturnsTheCurrentHitData()
    {
        $this->searchResponseIterator->method('valid')->willReturn(true);
        $this->searchResponseIterator->method('current')->willReturn([
            'hits' => [
                'hits'=> [
                    ['foo' => 'bar'],
                    ['baz' => 'qux']
                ]
            ]
        ]);

        $this->iterator->rewind();
        $this->assertEquals(['foo' => 'bar'], $this->iterator->current());
        $this->iterator->next();
        $this->assertEquals(['baz' => 'qux'], $this->iterator->current());
    }

    public function testTheKeyMethodReturnsTheCurrentHitIndex()
    {
        $this->searchResponseIterator->method('valid')->willReturn(true);
        $this->searchResponseIterator->method('current')->willReturn([
            'hits' => [
                'hits'=> [
                    ['foo' => 'bar'],
                    ['baz' => 'qux']
                ]
            ]
        ]);

        $this->iterator->rewind();
        $this->assertEquals(0, $this->iterator->key());
        $this->iterator->next();
        $this->assertEquals(1, $this->iterator->key());
    }

    public function testTheNextMethodShouldMoveThroughTheResultPages()
    {
        $firstPage = [
            'hits' => [
                'hits'=> [
                    ['foo' => 'bar'],
                    ['baz' => 'qux']
                ]
            ]
        ];

        $secondPage = [
            'hits' => [
                'hits'=> [
                    ['spoon' => 'fork'],
                    ['orange' => 'spork']
                ]
            ]
        ];

        $this->searchResponseIterator->method('valid')->willReturn(true);
        // `current` is called twice as part of the initial re-wind, then once for the first request for data
        // from the iterator, a fourth time for the second request, then two more times for the third and
        // fourth requests -- so we wind up with 6 calls to `current` for this process. Normally, we would be
        // advancing the index pointer only on the 3rd-6th calls (the first two are just part of the initial
        // data retrieval step when the iterator is re-wound).
        $this->searchResponseIterator->method('current')->willReturnOnConsecutiveCalls(
            $firstPage,
            $firstPage,
            $firstPage,
            $firstPage,
            $secondPage,
            $secondPage
        );

        $this->iterator->rewind();
        $this->assertEquals(0, $this->iterator->key());
        $this->assertEquals(['foo' => 'bar'], $this->iterator->current());
        $this->iterator->next();
        $this->assertEquals(1, $this->iterator->key());
        $this->assertEquals(['baz' => 'qux'], $this->iterator->current());

        $this->iterator->next();
        $this->assertEquals(0, $this->iterator->key());
        $this->assertEquals(['spoon' => 'fork'], $this->iterator->current());
        $this->iterator->next();
        $this->assertEquals(1, $this->iterator->key());
        $this->assertEquals(['orange' => 'spork'], $this->iterator->current());
    }
}
