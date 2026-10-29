<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\ZeroMq;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverFactoryInterface;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Psr\Log\LoggerInterface;

final class ZeroMqFactory implements DriverFactoryInterface
{
    /**
     * Connections open lazily on the first publish/receive, because PUB/SUB and
     * PUSH/PULL need separate sockets and either role may go unused.
     */
    public function create(ConnectionConfig $config, ?LoggerInterface $logger = null): DriverInterface
    {
        return new ZeroMqDriver($config, $logger);
    }
}
