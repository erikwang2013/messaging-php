<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Protocol\Nats;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Nats\NatsFactory;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class NatsDriverProtocolTest extends TestCase
{
    /** @var resource|null */
    private $process = null;

    /** @var array<int, resource> */
    private array $pipes = [];

    protected function tearDown(): void
    {
        foreach ($this->pipes as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }
        $this->pipes = [];

        if (is_resource($this->process)) {
            proc_terminate($this->process);
            proc_close($this->process);
        }
        $this->process = null;
    }

    public function test_connect_publish_and_receive_roundtrip(): void
    {
        $driver = $this->driver($this->startServer('normal'));
        $this->subscribe($driver, 'proto.roundtrip');

        $result = $driver->publish('proto.roundtrip', new Message('hello'));
        $message = $driver->receive('proto.roundtrip', new ReceiveOptions(500));

        self::assertSame('proto.roundtrip', $result->destination);
        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame($result->id, $message->id());

        $driver->close();
    }

    public function test_headers_and_key_survive_roundtrip(): void
    {
        $driver = $this->driver($this->startServer('normal'));
        $this->subscribe($driver, 'proto.headers');

        $driver->publish('proto.headers', new Message('payload', ['trace' => 'abc', 'attempt' => 2], 'key-1'));
        $message = $driver->receive('proto.headers', new ReceiveOptions(500));

        self::assertNotNull($message);
        self::assertSame(['trace' => 'abc', 'attempt' => '2'], $message->headers());
        self::assertSame('key-1', $message->key());

        $driver->close();
    }

    public function test_binary_and_empty_bodies_survive_roundtrip(): void
    {
        $driver = $this->driver($this->startServer('normal'));
        $this->subscribe($driver, 'proto.binary');

        $driver->publish('proto.binary', new Message("\x00\x01\xffbinary"));
        $driver->publish('proto.binary', new Message(''));

        $binary = $driver->receive('proto.binary', new ReceiveOptions(500));
        $empty = $driver->receive('proto.binary', new ReceiveOptions(500));

        self::assertNotNull($binary);
        self::assertSame("\x00\x01\xffbinary", $binary->body());
        self::assertNotNull($empty);
        self::assertSame('', $empty->body());

        $driver->close();
    }

    public function test_receive_returns_null_on_timeout(): void
    {
        $driver = $this->driver($this->startServer('normal'));
        $startedAt = microtime(true);

        self::assertNull($driver->receive('proto.empty', new ReceiveOptions(100)));
        self::assertLessThan(2.0, microtime(true) - $startedAt);

        $driver->close();
    }

    public function test_ack_is_unsupported_for_core_nats(): void
    {
        $driver = $this->driver($this->startServer('normal'));
        $this->subscribe($driver, 'proto.ack');

        $driver->publish('proto.ack', new Message('ack-me'));
        $message = $driver->receive('proto.ack', new ReceiveOptions(500));
        self::assertNotNull($message);

        $this->expectException(UnsupportedFeatureException::class);
        $message->ack();
    }

    public function test_buffer_limit_rejects_unread_messages_during_publish(): void
    {
        $driver = $this->driver($this->startServer('normal'), ['buffer_limit' => 1]);
        $this->subscribe($driver, 'proto.buffered');

        $driver->publish('proto.buffered', new Message('one'));

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('buffer_limit');
        $driver->publish('proto.buffered', new Message('two'));
    }

    public function test_buffer_limit_rejects_messages_for_other_destinations(): void
    {
        $driver = $this->driver($this->startServer('burst'), ['buffer_limit' => 1]);

        // The server pushes three messages; receive() returns the first and
        // leaves the rest unread.
        $first = $driver->receive('proto.b', new ReceiveOptions(500));
        self::assertNotNull($first);
        self::assertSame('burst-1', $first->body());

        // Receiving A drains the remaining B messages into B's buffer; the
        // second one exceeds buffer_limit and must fail instead of growing.
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('buffer_limit');
        $driver->receive('proto.a', new ReceiveOptions(500));
    }

    public function test_publish_reconnects_and_retries_after_idle_disconnect(): void
    {
        $driver = $this->driver($this->startServer('reconnect'));

        // The fake server closed the connection right after the handshake;
        // publish must reconnect and retry once.
        $result = $driver->publish('proto.reconnect', new Message('after-reconnect'));
        self::assertNotNull($result->id);

        // Subscriptions are connection-scoped and must be rebuilt afterwards.
        $this->subscribe($driver, 'proto.reconnect');
        $driver->publish('proto.reconnect', new Message('second'));
        $message = $driver->receive('proto.reconnect', new ReceiveOptions(500));

        self::assertNotNull($message);
        self::assertSame('second', $message->body());

        $driver->close();
    }

    public function test_authorization_errors_are_reported(): void
    {
        $port = $this->startServer('error');

        $this->expectException(AuthenticationException::class);
        $this->driver($port);
    }

    public function test_protocol_errors_are_reported(): void
    {
        $port = $this->startServer('protocol-error');

        $this->expectException(ProtocolException::class);
        $this->driver($port);
    }

    public function test_handshake_times_out_against_silent_server(): void
    {
        $port = $this->startServer('silent');

        $this->expectException(TimeoutException::class);
        $this->driver($port, ['timeout' => 0.2]);
    }

    public function test_server_ping_is_answered_with_pong(): void
    {
        $driver = $this->driver($this->startServer('heartbeat'));

        $message = $driver->receive('proto.heartbeat', new ReceiveOptions(1000));

        self::assertNotNull($message);
        self::assertSame('heartbeat-ok', $message->body());

        $driver->close();
    }

    public function test_server_without_header_support_negotiates_headers_off(): void
    {
        $driver = $this->driver($this->startServer('no-headers'));

        self::assertFalse($driver->capabilities()->headers);

        $this->subscribe($driver, 'proto.plain');
        $driver->publish('proto.plain', new Message('plain'));
        $message = $driver->receive('proto.plain', new ReceiveOptions(500));
        self::assertNotNull($message);
        self::assertSame('plain', $message->body());

        $driver->close();
    }

    public function test_headers_are_rejected_when_server_lacks_support(): void
    {
        $driver = $this->driver($this->startServer('no-headers'));

        $this->expectException(UnsupportedFeatureException::class);
        $driver->publish('proto.plain', new Message('body', ['trace' => 'abc']));
    }

    /**
     * Core NATS only delivers to active subscriptions, so tests must subscribe
     * before publishing.
     */
    private function subscribe(DriverInterface $driver, string $subject): void
    {
        self::assertNull($driver->receive($subject, new ReceiveOptions(10)));
    }

    /**
     * @param array<string, mixed> $options
     */
    private function driver(int $port, array $options = []): DriverInterface
    {
        return (new NatsFactory())->create(ConnectionConfig::fromArray('protocol-nats', $options + [
            'driver' => 'nats',
            'host' => '127.0.0.1',
            'port' => $port,
            'timeout' => 1.0,
        ]));
    }

    private function startServer(string $mode): int
    {
        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $process = proc_open(
            [PHP_BINARY, __DIR__ . '/fixtures/fake_nats_server.php', $mode],
            $descriptors,
            $pipes,
        );
        self::assertIsResource($process);

        $this->process = $process;
        $this->pipes = $pipes;

        $line = fgets($pipes[1]);
        self::assertIsString($line, 'Fake NATS server did not report a port.');
        self::assertStringStartsWith('PORT ', $line);

        return (int) substr(trim($line), 5);
    }
}
