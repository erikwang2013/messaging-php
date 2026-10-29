<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

final class EventHubsFactory extends KafkaFactory
{
    public function __construct()
    {
        parent::__construct('eventhubs');
    }
}
