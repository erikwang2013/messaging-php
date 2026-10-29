<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverFactoryInterface;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Psr\Log\LoggerInterface;

class KafkaFactory implements DriverFactoryInterface
{
    public function __construct(protected string $alias = 'kafka')
    {
    }

    public function create(ConnectionConfig $config, ?LoggerInterface $logger = null): DriverInterface
    {
        $driver = $config->driver();
        $alias = isset(KafkaConfig::ALIASES[$driver]) ? $driver : $this->alias;

        if ($alias === 'eventhubs') {
            $logger?->warning('Azure Event Hubs requires SASL/PLAIN, which this Kafka driver does not implement.');
        }

        return new KafkaDriver(KafkaConfig::fromConnection($config, $alias), $logger);
    }
}
