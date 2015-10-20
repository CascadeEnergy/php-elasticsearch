<?php

namespace CascadeEnergy\ElasticSearch;

use Elasticsearch\Client;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

/**
 * This factory creates Bulk objects that are configured with the given Elasticsearch client and EventDispatcher.
 */
class BulkFactory
{
    /** @var Client */
    private $elasticSearchClient;

    /** @var EventDispatcherInterface|null */
    private $eventDispatcher;

    /**
     * @param Client $elasticSearchClient The Elasticsearch client
     */
    public function __construct(Client $elasticSearchClient)
    {
        $this->elasticSearchClient = $elasticSearchClient;
    }

    /**
     * @param EventDispatcherInterface $eventDispatcher The event dispatcher object
     */
    public function setEventDispatcher(EventDispatcherInterface $eventDispatcher)
    {
        $this->eventDispatcher = $eventDispatcher;
    }

    /**
     * @param string $indexName The index against which bulk operations will be performed
     * @param string $type The type against which bulk operations will be performed
     *
     * @return Bulk A Bulk instance configured with the given index and type
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
