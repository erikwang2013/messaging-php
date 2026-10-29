<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Nats;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Driver\Nats\NatsFactory;
use Erikwang2013\Messaging\Exception\ConnectionException;
use PHPUnit\Framework\TestCase;

final class NatsFactoryTest extends TestCase
{
    public function test_host_is_required(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new NatsFactory())->create(ConnectionConfig::fromArray('nats', ['driver' => 'nats']));
    }

    public function test_unreachable_server_throws_connection_exception(): void
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
        self::assertIsResource($server, $errorMessage);

        $address = stream_socket_get_name($server, false);
        self::assertIsString($address);
        $separator = (int) strrpos($address, ':');
        $port = (int) substr($address, $separator + 1);
        fclose($server);

        $this->expectException(ConnectionException::class);

        (new NatsFactory())->create(ConnectionConfig::fromArray('nats', [
            'driver' => 'nats',
            'host' => '127.0.0.1',
            'port' => $port,
            'timeout' => 0.2,
        ]));
    }
}
