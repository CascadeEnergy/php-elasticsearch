<?php

namespace CascadeEnergy\ElasticSearch;

use Elasticsearch\Client;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

class BulkFactory
{
    /** @var Client */
    private $elasticSearchClient;

    /** @var EventDispatcherInterface|null */
    private $eventDispatcher;

    /**
     * @param Client $elasticSearchClient
     */
    public function __construct(Client $elasticSearchClient)
    {
        $this->elasticSearchClient = $elasticSearchClient;
    }

    /**
     * @param EventDispatcherInterface $eventDispatcher
     */
    public function setEventDispatcher(EventDispatcherInterface $eventDispatcher)
    {
        $this->eventDispatcher = $eventDispatcher;
    }

    /**
     * @param string $indexName
     * @param string $type
     *
     * @return Bulk
     */
    public function createBulk($indexName, $type)
    {
        $bulk = new Bulk($this->elasticSearchClient, $indexName, $type);

        if ($this->eventDispatcher) {
            $bulk->setEventDispatcher($this->eventDispatcher);
        }

        return $bulk;
    }
}
