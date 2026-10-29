<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Support;

use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Support\Socket;
use PHPUnit\Framework\TestCase;

final class SocketTest extends TestCase
{
    /** @var resource|null */
    private $server = null;

    protected function tearDown(): void
    {
        if (is_resource($this->server)) {
            fclose($this->server);
        }
    }

    public function test_write_and_read_roundtrip(): void
    {
        [$host, $port] = $this->listen();
        $client = Socket::connect($host, $port, 1.0);
        $peer = stream_socket_accept($this->server, 1.0);
        self::assertIsResource($peer);

        $client->write('ping');
        self::assertSame('ping', fread($peer, 4));

        fwrite($peer, 'pong');
        self::assertSame('pong', $client->read(4));

        $client->close();
        fclose($peer);
    }

    public function test_connect_failure_throws(): void
    {
        [$host, $port] = $this->listen();
        fclose($this->server);
        $this->server = null;

        $this->expectException(ConnectionException::class);

        Socket::connect($host, $port, 0.2);
    }

    /**
     * @return array{string, int}
     */
    private function listen(): array
    {
        $errorMessage = '';
        $this->server = stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
        self::assertIsResource($this->server, $errorMessage);

        $address = stream_socket_get_name($this->server, false);
        self::assertIsString($address);
        $separator = strrpos($address, ':');
        self::assertNotFalse($separator);

        return [substr($address, 0, $separator), (int) substr($address, $separator + 1)];
    }
}
