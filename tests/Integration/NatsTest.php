<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Integration;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Driver\Nats\NatsFactory;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Tests\Contract\DriverContractTestCase;
use PHPUnit\Framework\Attributes\Group;

#[Group('integration')]
final class NatsTest extends DriverContractTestCase
{
    protected function createDriver(): DriverInterface
    {
        $host = getenv('MESSAGING_NATS_HOST');
        $host = is_string($host) && $host !== '' ? $host : '127.0.0.1';

        $port = getenv('MESSAGING_NATS_PORT');
        $port = is_string($port) && $port !== '' ? (int) $port : 4222;

        try {
            return (new NatsFactory())->create(ConnectionConfig::fromArray('nats', [
                'driver' => 'nats',
                'host' => $host,
                'port' => $port,
                'timeout' => 2.0,
            ]));
        } catch (ConnectionException $exception) {
            self::markTestSkipped(sprintf(
                'NATS is not available at %s:%d (%s).',
                $host,
                $port,
                $exception->getMessage(),
            ));
        }
    }
}
