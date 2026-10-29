<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Integration;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Driver\RabbitMq\RabbitMqFactory;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Support\Socket;
use Erikwang2013\Messaging\Tests\Contract\DriverContractTestCase;
use PHPUnit\Framework\Attributes\Group;

#[Group('integration')]
final class RabbitMqTest extends DriverContractTestCase
{
    protected function createDriver(): DriverInterface
    {
        $host = getenv('MESSAGING_RABBITMQ_HOST') ?: '127.0.0.1';
        $port = (int) (getenv('MESSAGING_RABBITMQ_PORT') ?: 5672);

        try {
            $probe = Socket::connect($host, $port, 1.0);
            $probe->close();
        } catch (ConnectionException $exception) {
            self::markTestSkipped(sprintf(
                'RabbitMQ is not reachable at %s:%d: %s',
                $host,
                $port,
                $exception->getMessage(),
            ));
        }

        try {
            return (new RabbitMqFactory())->create(ConnectionConfig::fromArray('rabbitmq', [
                'driver' => 'rabbitmq',
                'host' => $host,
                'port' => $port,
                'username' => getenv('MESSAGING_RABBITMQ_USER') ?: 'guest',
                'password' => getenv('MESSAGING_RABBITMQ_PASS') ?: 'guest',
                'vhost' => getenv('MESSAGING_RABBITMQ_VHOST') ?: '/',
                'timeout' => 5.0,
                'heartbeat' => 60,
            ]));
        } catch (ConnectionException $exception) {
            self::markTestSkipped('RabbitMQ handshake failed: ' . $exception->getMessage());
        }
    }
}
