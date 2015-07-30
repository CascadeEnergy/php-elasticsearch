<?php

namespace CascadeEnergy\ElasticSearch\Events;

use Symfony\Component\EventDispatcher\Event;

class BulkFlushEvent extends Event
{
    /** @var string */
    private $index;

    /** @var int */
    private $rowCount;

    /** @var string */
    private $type;

    public function __construct($index, $type, $rowCount)
    {
        $this->index = $index;
        $this->rowCount = $rowCount;
        $this->type = $type;
    }

    /**
     * @return string
     */
    public function getIndex()
    {
        return $this->index;
    }

    /**
     * @return int
     */
    public function getRowCount()
    {
        return $this->rowCount;
    }

    /**
     * @return string
     */
    public function getType()
    {
        return $this->type;
    }
}
