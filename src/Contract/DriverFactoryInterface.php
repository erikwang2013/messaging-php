<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Contract;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Psr\Log\LoggerInterface;

interface DriverFactoryInterface
{
    public function create(ConnectionConfig $config, ?LoggerInterface $logger = null): DriverInterface;
}
