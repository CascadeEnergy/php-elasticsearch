<?php

namespace CascadeEnergy\ElasticSearch\Events;

use Symfony\Component\EventDispatcher\Event;

/**
 * An event which is emitted whenever a Bulk instance flushes its current content to Elasticsearch.
 */
class BulkFlushEvent extends Event
{
    /** @var string */
    private $index;

    /** @var int */
    private $rowCount;

    /** @var string */
    private $type;

    /**
     * @param string $index The index data was flushed to
     * @param string $type The type of data flushed
     * @param int $rowCount The number of items flushed
     */
    public function __construct($index, $type, $rowCount)
    {
        $this->index = $index;
        $this->rowCount = $rowCount;
        $this->type = $type;
    }

    /**
     * @return string The index data was flushed to
     */
    public function getIndex()
    {
        return $this->index;
    }

    /**
     * @return int The number of items flushed
     */
    public function getRowCount()
    {
        return $this->rowCount;
    }

    /**
     * @return string The type of data flushed
     */
    public function getType()
    {
        return $this->type;
    }
}
