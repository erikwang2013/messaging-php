<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Nsq;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverFactoryInterface;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Psr\Log\LoggerInterface;

final class NsqFactory implements DriverFactoryInterface
{
    public function create(ConnectionConfig $config, ?LoggerInterface $logger = null): DriverInterface
    {
        $host = $config->requireString('host');

        $port = $config->get('port', 4150);
        if (!is_int($port) || $port < 1 || $port > 65535) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [port] must be an integer between 1 and 65535.',
                $config->name(),
            ));
        }

        $channel = $config->get('channel', 'default');
        if (!is_string($channel)) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [channel] must be a string.',
                $config->name(),
            ));
        }
        NsqProtocol::assertName($channel, 'channel');

        $timeout = $config->get('timeout', 5.0);
        if (!self::isPositiveNumber($timeout)) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [timeout] must be a positive number.',
                $config->name(),
            ));
        }

        $requeueDelay = $config->get('requeue_delay_ms', 0);
        if (!is_int($requeueDelay) || $requeueDelay < 0) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [requeue_delay_ms] must be a non-negative integer.',
                $config->name(),
            ));
        }

        return new NsqDriver($host, $port, $channel, (float) $timeout, $requeueDelay, $logger);
    }

    private static function isPositiveNumber(mixed $value): bool
    {
        return (is_int($value) || is_float($value)) && $value > 0;
    }
}
