<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

final class ConfluentFactory extends KafkaFactory
{
    public function __construct()
    {
        parent::__construct('confluent');
    }
}
