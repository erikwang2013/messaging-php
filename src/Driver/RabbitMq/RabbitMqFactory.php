<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RabbitMq;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverFactoryInterface;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Psr\Log\LoggerInterface;

final class RabbitMqFactory implements DriverFactoryInterface
{
    /**
     * Connects eagerly so invalid endpoints and credentials fail at
     * Manager::connection() time instead of on the first publish.
     */
    public function create(ConnectionConfig $config, ?LoggerInterface $logger = null): DriverInterface
    {
        $driver = new RabbitMqDriver($config, $logger);
        $driver->connect();

        return $driver;
    }
}
