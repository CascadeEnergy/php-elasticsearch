<?php

namespace CascadeEnergy\ElasticSearch;

use CascadeEnergy\ElasticSearch\Events\BulkFlushEvent;
use CascadeEnergy\ElasticSearch\Exceptions\PartialFailureException;
use CascadeEnergy\SymfonyEventDispatcher\EventDispatcherConsumerTrait;
use Elasticsearch\Client;

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

    public function addItem($id, $item)
    {
        $this->itemList[] = ['index' => ['_id' => $id]];
        $this->itemList[] = $item;
        $this->itemCount++;

        if ($this->autoFlushThreshold > 0 && $this->getItemCount() >= $this->autoFlushThreshold) {
            $this->flush();
        }
    }

    public function getItemCount()
    {
        return $this->itemCount;
    }

    public function begin()
    {
        $this->flush();
    }

    public function end()
    {
        $this->flush();
    }

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

        if (!empty($result->errors)) {
            throw new PartialFailureException(count($result->errors) . " items failed.", $result->errors);
        }

        $this->itemList = [];
        $this->itemCount = 0;
    }
}
