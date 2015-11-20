<?php

namespace CascadeEnergy\ElasticSearch;

use CascadeEnergy\ElasticSearch\Events\BulkFlushEvent;
use CascadeEnergy\ElasticSearch\Exceptions\PartialFailureException;
use CascadeEnergy\SymfonyEventDispatcher\EventDispatcherConsumerTrait;
use Elasticsearch\Client;

/**
 * This class manages collections of items for bulk operations against an Elasticsearch cluster.
 *
 * The normal use case is roughly as follows:
 *
 * $bulk = new Bulk('some-index', 'some-type');
 *
 * $bulk->begin();
 *
 * while (areThereThingsToDo()) {
 *   $bulk->addItem($somethingToIndexInElasticSearch);
 * }
 *
 * $bulk->end();
 *
 * Note that the Bulk class will automatically flush documents to Elasticsearch when the number of documents
 * is greater than or equal to the configurable auto-flush threshold (the ideal setting for this threshold
 * depends on the size of your documents, total number of documents, and cluster configuration).
 *
 * Calling $bulk->end() or $bulk->flush() after adding all the desired items is required to ensure that any items
 * not included in an automatic flush are correctly sent to Elasticsearch in a final bulk operation.
 */
class Bulk
{
    const DEFAULT_AUTO_FLUSH_THRESHOLD = 2500;

    use EventDispatcherConsumerTrait;

    /** @var int The number of items at which an auto-flush is triggered; may be 0 to disable auto-flushing */
    private $autoFlushThreshold;

    /** @var Client */
    private $elasticSearch;

    /** @var string */
    private $index;

    /** @var int */
    private $itemCount = 0;

    /** @var array */
    private $itemList = [];

    /** @var string */
    private $type;

    /**
     * @param Client $elasticSearch The elastic search client
     * @param string $index The name of the index the bulk operations will be performed against
     * @param string $type The name of the type the bulk operations will be performed against
     * @param int $autoFlushThreshold The number of items to collect before automatically flushing the operation
     */
    public function __construct(
        Client $elasticSearch,
        $index,
        $type,
        $autoFlushThreshold = self::DEFAULT_AUTO_FLUSH_THRESHOLD
    ) {
        $this->autoFlushThreshold = $autoFlushThreshold;
        $this->elasticSearch = $elasticSearch;
        $this->index = $index;
        $this->type = $type;
    }

    /**
     * Adds an item to the bulk operation.
     *
     * @param $id
     * @param $item
     * @param string $operation
     * @param string|null $index The index to operate on -- overrides the default for this Bulk object
     * @param string|null $type The type to operate on -- overrides the default for this Bulk object
     *
     * @throws PartialFailureException
     */
    public function addItem($id, $item, $operation = 'index', $index = null, $type = null)
    {
        $metadata = ['_id' => $id];

        if (!is_null($index)) {
             $metadata['_index'] = $index;
        }

        if (!is_null($type)) {
            $metadata['_type'] = $type;
        }

        $this->itemList[] = [$operation => $metadata];
        $this->itemList[] = $item;
        $this->itemCount++;

        if ($this->autoFlushThreshold > 0 && $this->getItemCount() >= $this->autoFlushThreshold) {
            $this->flush();
        }
    }

    /**
     * @return int The number of pending items in the operation
     */
    public function getItemCount()
    {
        return $this->itemCount;
    }

    /**
     * Starts the bulk operation by flushing any items currently in it.
     *
     * @throws PartialFailureException
     */
    public function begin()
    {
        $this->flush();
    }

    /**
     * Ends the bulk operation by flushing any items currently in it.
     *
     * @throws PartialFailureException
     */
    public function end()
    {
        $this->flush();
    }

    /**
     * Flushes the items in this Bulk object to Elasticsearch. If there are no items, this method does nothing.
     *
     * (In particular, note that if no items are in the Bulk object, the bulk flush event is not fired.)
     *
     * @throws PartialFailureException
     */
    public function flush()
    {
        // We might have nothing to do
        if ($this->getItemCount() == 0) {
            return;
        }

        $this->dispatchEvent(
            Events::BULK_FLUSH,
            new BulkFlushEvent($this->index, $this->type, $this->getItemCount())
        );

        $params = [
            'index' => $this->index,
            'type' => $this->type,
            'body' => $this->itemList
        ];

        $result = $this->elasticSearch->bulk($params);

        if (boolval($result->errors)) {
            throw new PartialFailureException("Some items failed.");
        }

        $this->itemList = [];
        $this->itemCount = 0;
    }
}
