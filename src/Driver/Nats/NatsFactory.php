<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Nats;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverFactoryInterface;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Psr\Log\LoggerInterface;

final class NatsFactory implements DriverFactoryInterface
{
    /**
     * Connects eagerly so invalid endpoints and credentials fail at
     * Manager::connection() time instead of on the first publish.
     */
    public function create(ConnectionConfig $config, ?LoggerInterface $logger = null): DriverInterface
    {
        $driver = new NatsDriver($config, $logger);
        $driver->connect();

        return $driver;
    }
}
