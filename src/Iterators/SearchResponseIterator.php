<?php

namespace CascadeEnergy\ElasticSearch\Iterators;

use Elasticsearch\Client;

/**
 * Based on the original iterator class from the Elasticsearch package.
 *
 * Updated to allow the scroll ID to be explicitly cleared, which makes it possible to better control and handle
 * errors (particularly 404 exceptions) that might occur when the scroll ID is cleared.
 */
class SearchResponseIterator implements \Iterator
{
    /**
     * @var Client
     */
    private $client;

    /**
     * @var array
     */
    private $params;

    /**
     * @var int
     */
    private $currentKey;

    /**
     * @var array
     */
    private $currentScrolledResponse;

    /**
     * @var string
     */
    private $scrollId;

    /**
     * @var string
     */
    private $scrollTtl;

    /**
     * Constructor
     *
     * @param Client $client
     * @param array  $params  Associative array of parameters
     * @see   Client::search()
     */
    public function __construct(Client $client, array $params)
    {
        $this->client = $client;
        $this->params = $params;

        if (isset($params['scroll'])) {
            $this->scrollTtl = $params['scroll'];
        }
    }

    /**
     * Sets the time to live duration of a scroll window
     *
     * @param  string $timeToLive
     * @return $this
     */
    public function setScrollTimeout($timeToLive)
    {
        $this->scrollTtl = $timeToLive;
        return $this;
    }

    /**
     * Clears the current scroll window if there is a scroll_id stored
     *
     * @return void
     */
    public function clearScroll()
    {
        if (!empty($this->scrollId)) {
            $scrollId = $this->scrollId;
            $this->scrollId = null;

            $this->client->clearScroll(['scroll_id' => $scrollId]);
        }
    }

    /**
     * Rewinds the iterator by performing the initial search.
     *
     * The "search_type" parameter will determine if the first "page" contains
     * hits or if the first page contains just a "scroll_id"
     *
     * @return void
     * @see    Iterator::rewind()
     */
    public function rewind()
    {
        $this->clearScroll();
        $this->currentKey = 0;
        $this->currentScrolledResponse = $this->client->search($this->params);
        $this->scrollId = $this->currentScrolledResponse['_scroll_id'];
    }

    /**
     * Fetches every "page" after the first one using the latest "scroll_id"
     *
     * @return void
     * @see    Iterator::next()
     */
    public function next()
    {
        $this->currentKey++;
        $this->currentScrolledResponse = $this->client->scroll(
            array(
                'scroll_id' => $this->scrollId,
                'scroll'    => $this->scrollTtl
            )
        );
        $this->scrollId = $this->currentScrolledResponse['_scroll_id'];
    }

    /**
     * Returns a boolean value indicating if the current page is valid or not
     * based on the number of hits in the page considering that the first page
     * might not include any hits
     *
     * @return bool
     * @see    Iterator::valid()
     */
    public function valid()
    {
        return ($this->currentKey === 0) || isset($this->currentScrolledResponse['hits']['hits'][0]);
    }

    /**
     * Returns the current "page"
     *
     * @return array
     * @see    Iterator::current()
     */
    public function current()
    {
        return $this->currentScrolledResponse;
    }

    /**
     * Returns the current "page number" of the current "page"
     *
     * @return int
     * @see    Iterator::key()
     */
    public function key()
    {
        return $this->currentKey;
    }
}
