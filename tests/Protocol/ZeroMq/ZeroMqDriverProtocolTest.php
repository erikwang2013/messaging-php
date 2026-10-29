<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Protocol\ZeroMq;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\ZeroMq\ZeroMqFactory;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class ZeroMqDriverProtocolTest extends TestCase
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

    public function test_pubsub_publish_and_receive_roundtrip(): void
    {
        $driver = $this->driver($this->startPeer('relay'));
        $this->subscribe($driver, 'proto.topic');

        $result = $driver->publish('proto.topic', new Message('hello'));
        $message = $driver->receive('proto.topic', new ReceiveOptions(1000));

        self::assertSame('proto.topic', $result->destination);
        self::assertNull($result->id);
        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertNull($message->id());
        self::assertSame([], $message->headers());
        self::assertSame('proto.topic', $message->raw()['destination']);

        $driver->close();
    }

    public function test_pushpull_publish_and_receive_roundtrip(): void
    {
        $driver = $this->driver($this->startPeer('relay'), ['pattern' => 'pushpull']);

        self::assertNull($driver->receive('ignored', new ReceiveOptions(10)));
        $driver->publish('ignored', new Message('hello'));
        $message = $driver->receive('ignored', new ReceiveOptions(1000));

        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame([], $message->headers());

        $driver->close();
    }

    public function test_binary_and_empty_bodies_survive_roundtrip(): void
    {
        $driver = $this->driver($this->startPeer('relay'));

        $this->subscribe($driver, 'proto.binary');
        $driver->publish('proto.binary', new Message("\x00\x01\xffbinary"));
        $driver->publish('proto.binary', new Message(''));

        $binary = $driver->receive('proto.binary', new ReceiveOptions(1000));
        $empty = $driver->receive('proto.binary', new ReceiveOptions(1000));

        self::assertNotNull($binary);
        self::assertSame("\x00\x01\xffbinary", $binary->body());
        self::assertNotNull($empty);
        self::assertSame('', $empty->body());

        $driver->close();
    }

    public function test_multipart_message_is_reassembled(): void
    {
        $driver = $this->driver($this->startPeer('multipart'));

        $message = $driver->receive('proto.multi', new ReceiveOptions(1000));

        self::assertNotNull($message);
        self::assertSame('hello', $message->body());

        $driver->close();
    }

    public function test_timeout_mid_frame_resumes_without_losing_the_stream(): void
    {
        $driver = $this->driver($this->startPeer('split-frame'));

        // First call registers the subscription and times out after the flags byte.
        self::assertNull($driver->receive('proto.split', new ReceiveOptions(10)));
        $message = $driver->receive('proto.split', new ReceiveOptions(1000));

        self::assertNotNull($message);
        self::assertSame('hello', $message->body());

        $driver->close();
    }

    public function test_messages_for_other_destinations_are_buffered(): void
    {
        $driver = $this->driver($this->startPeer('relay'));

        $this->subscribe($driver, 'proto.a');
        $this->subscribe($driver, 'proto.b');

        $driver->publish('proto.b', new Message('bee'));
        $driver->publish('proto.a', new Message('aye'));

        $first = $driver->receive('proto.a', new ReceiveOptions(1000));
        $second = $driver->receive('proto.b', new ReceiveOptions(1000));

        self::assertNotNull($first);
        self::assertSame('aye', $first->body());
        self::assertNotNull($second);
        self::assertSame('bee', $second->body());

        $driver->close();
    }

    public function test_buffer_limit_throws_for_non_target_destination(): void
    {
        $driver = $this->driver($this->startPeer('relay'), ['buffer_limit' => 1]);

        $this->subscribe($driver, 'proto.a');
        $this->subscribe($driver, 'proto.b');

        $driver->publish('proto.b', new Message('one'));
        $driver->publish('proto.b', new Message('two'));

        $this->expectException(ProtocolException::class);
        $driver->receive('proto.a', new ReceiveOptions(1000));
    }

    public function test_receive_returns_null_on_timeout(): void
    {
        $driver = $this->driver($this->startPeer('relay'));
        $startedAt = microtime(true);

        self::assertNull($driver->receive('proto.empty', new ReceiveOptions(100)));
        self::assertLessThan(2.0, microtime(true) - $startedAt);

        $driver->close();
    }

    public function test_ack_and_reject_are_unsupported(): void
    {
        $driver = $this->driver($this->startPeer('relay'));
        $this->subscribe($driver, 'proto.ack');
        $driver->publish('proto.ack', new Message('ack-me'));
        $message = $driver->receive('proto.ack', new ReceiveOptions(1000));

        self::assertNotNull($message);

        try {
            $message->ack();
            self::fail('ack() should not be supported.');
        } catch (UnsupportedFeatureException) {
            // expected
        }

        $this->expectException(UnsupportedFeatureException::class);
        $message->reject(true);
    }

    public function test_malformed_greeting_is_rejected(): void
    {
        $driver = $this->driver($this->startPeer('bad-greeting'));

        $this->expectException(ProtocolException::class);
        $driver->receive('proto.bad', new ReceiveOptions(1000));
    }

    public function test_malformed_frame_is_rejected(): void
    {
        $driver = $this->driver($this->startPeer('bad-frame'));

        $this->expectException(ProtocolException::class);
        $driver->receive('proto.bad', new ReceiveOptions(1000));
    }

    public function test_incompatible_peer_socket_type_is_rejected(): void
    {
        $driver = $this->driver($this->startPeer('wrong-type'));

        $this->expectException(ProtocolException::class);
        $driver->receive('proto.bad', new ReceiveOptions(1000));
    }

    public function test_data_frame_instead_of_ready_is_rejected(): void
    {
        $driver = $this->driver($this->startPeer('no-ready'));

        $this->expectException(ProtocolException::class);
        $driver->receive('proto.bad', new ReceiveOptions(1000));
    }

    public function test_silent_peer_times_out(): void
    {
        $driver = $this->driver($this->startPeer('silent'), ['timeout' => 0.2]);

        $this->expectException(TimeoutException::class);
        $driver->receive('proto.silent', new ReceiveOptions(1000));
    }

    /**
     * PUB/SUB only delivers to subscriptions established before the publish,
     * so protocol tests subscribe first.
     */
    private function subscribe(DriverInterface $driver, string $destination): void
    {
        self::assertNull($driver->receive($destination, new ReceiveOptions(10)));
    }

    /**
     * @param array<string, mixed> $options
     */
    private function driver(int $port, array $options = []): DriverInterface
    {
        return (new ZeroMqFactory())->create(ConnectionConfig::fromArray('protocol-zeromq', $options + [
            'driver' => 'zeromq',
            'host' => '127.0.0.1',
            'port' => $port,
            'timeout' => 1.0,
        ]));
    }

    private function startPeer(string $mode): int
    {
        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $process = proc_open(
            [PHP_BINARY, __DIR__ . '/fixtures/fake_zmtp_peer.php', $mode],
            $descriptors,
            $pipes,
        );
        self::assertIsResource($process);

        $this->process = $process;
        $this->pipes = $pipes;

        $line = fgets($pipes[1]);
        self::assertIsString($line, 'Fake ZMTP peer did not report a port.');
        self::assertStringStartsWith('PORT ', $line);

        return (int) substr(trim($line), 5);
    }
}
