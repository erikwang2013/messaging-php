<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RocketMq;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverFactoryInterface;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Psr\Log\LoggerInterface;

final class RocketMqFactory implements DriverFactoryInterface
{
    public function create(ConnectionConfig $config, ?LoggerInterface $logger = null): DriverInterface
    {
        return new RocketMqDriver(RocketMqConfig::fromConnection($config), $logger);
    }
}
