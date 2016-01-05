<?php

namespace CascadeEnergy\ElasticSearch\Iterators;

/**
 * Based very closely on the SearchHitIterator class in the Elasticsearch package.
 */
class SearchHitIterator implements \Iterator
{
    /**
     * @var SearchResponseIterator
     */
    private $searchResponseIterator;

    /**
     * @var int
     */
    protected $currentKey;

    /**
     * @var int
     */
    protected $currentHitIndex;

    /**
     * @var array|null
     */
    protected $currentHitData;

    /**
     * Constructor
     *
     * @param SearchResponseIterator $searchResponseIterator
     */
    public function __construct(SearchResponseIterator $searchResponseIterator)
    {
        $this->searchResponseIterator = $searchResponseIterator;
    }

    /**
     * Rewinds the internal SearchResponseIterator and itself
     *
     * @return void
     * @see    Iterator::rewind()
     */
    public function rewind()
    {
        $this->currentKey = 0;
        $this->searchResponseIterator->rewind();

        // The first page may be empty. In that case, the next page is fetched.
        $current_page = $this->searchResponseIterator->current();
        if($this->searchResponseIterator->valid() && empty($current_page['hits']['hits'])) {
            $this->searchResponseIterator->next();
        }

        $this->readPageData();
    }

    /**
     * Advances pointer of the current hit to the next one in the current page. If there
     * isn't a next hit in the current page, then it advances the current page and moves the
     * pointer to the first hit in the page.
     *
     * @return void
     * @see    Iterator::next()
     */
    public function next()
    {
        $this->currentKey++;
        $this->currentHitIndex++;
        $current_page = $this->searchResponseIterator->current();
        if(isset($current_page['hits']['hits'][$this->currentHitIndex])) {
            $this->currentHitData = $current_page['hits']['hits'][$this->currentHitIndex];
        } else {
            $this->searchResponseIterator->next();
            $this->readPageData();
        }
    }

    /**
     * Returns a boolean indicating whether or not the current pointer has valid data
     *
     * @return bool
     * @see    Iterator::valid()
     */
    public function valid()
    {
        return is_array($this->currentHitData);
    }

    /**
     * Returns the current hit
     *
     * @return array
     * @see    Iterator::current()
     */
    public function current()
    {
        return $this->currentHitData;
    }

    /**
     * Returns the current hit index. The hit index spans all pages.
     *
     * @return int
     * @see    Iterator::key()
     */
    public function key()
    {
        return $this->currentHitIndex;
    }

    /**
     * Advances the internal SearchResponseIterator and resets the current_hit_index to 0
     *
     * @internal
     */
    private function readPageData()
    {
        if($this->searchResponseIterator->valid()) {
            $current_page = $this->searchResponseIterator->current();
            $this->currentHitIndex = 0;
            $this->currentHitData = $current_page['hits']['hits'][$this->currentHitIndex];
        } else {
            $this->currentHitData = null;
        }

    }
}
