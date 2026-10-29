<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Protocol\Gcp;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Gcp\GcpPubSubDriver;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\RequestException;
use PHPUnit\Framework\TestCase;

final class GcpPubSubProtocolTest extends TestCase
{
    private const FIXTURE = __DIR__ . '/fixtures/fake_pubsub_server.php';

    /** @var resource|null */
    private $process = null;

    /** @var array<int, resource> */
    private array $pipes = [];

    private ?GcpPubSubDriver $driver = null;

    protected function tearDown(): void
    {
        $this->driver?->close();
        $this->driver = null;

        if (is_resource($this->process)) {
            proc_terminate($this->process);
        }

        foreach ($this->pipes as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }
        $this->pipes = [];

        if (is_resource($this->process)) {
            proc_close($this->process);
            $this->process = null;
        }
    }

    public function test_publish_and_receive_roundtrip(): void
    {
        $driver = $this->brokerDriver();

        $result = $driver->publish('orders', new Message("\x00hello", ['trace' => 'abc'], 'key-1'));
        self::assertSame('orders', $result->destination);
        self::assertSame('m-1', $result->id);

        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);
        self::assertSame("\x00hello", $message->body());
        self::assertSame(['trace' => 'abc'], $message->headers());
        self::assertSame('key-1', $message->key());
        self::assertSame('m-1', $message->id());
        self::assertSame(1, $message->attempts());
        $message->ack();
    }

    public function test_ack_removes_message(): void
    {
        $driver = $this->brokerDriver();
        $driver->publish('orders', new Message('ack-me'));

        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);
        $message->ack();

        self::assertNull($driver->receive('orders', new ReceiveOptions(5)));
    }

    public function test_reject_with_requeue_redelivers_message(): void
    {
        $driver = $this->brokerDriver();
        $driver->publish('orders', new Message('retry-me'));

        $first = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($first);
        $first->reject(true);

        $second = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($second);
        self::assertSame('retry-me', $second->body());
        $second->ack();
    }

    public function test_reject_without_requeue_discards_message(): void
    {
        $driver = $this->brokerDriver();
        $driver->publish('orders', new Message('drop-me'));

        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);
        $message->reject(false);

        self::assertNull($driver->receive('orders', new ReceiveOptions(5)));
    }

    public function test_receive_returns_null_when_the_subscription_is_empty(): void
    {
        $driver = $this->brokerDriver();

        self::assertNull($driver->receive('orders', new ReceiveOptions(5)));
    }

    public function test_server_error_is_reported_as_a_request_error(): void
    {
        $driver = $this->brokerDriver('error');

        try {
            $driver->publish('orders', new Message('hello'));
            self::fail('Expected a RequestException.');
        } catch (RequestException $exception) {
            self::assertSame(500, $exception->statusCode());
            self::assertStringContainsString('emulator exploded', $exception->getMessage());
        }
    }

    public function test_non_json_response_is_reported_as_a_protocol_error(): void
    {
        $driver = $this->brokerDriver('malformed');

        $this->expectException(ProtocolException::class);

        $driver->publish('orders', new Message('hello'));
    }

    private function brokerDriver(string $mode = 'normal'): GcpPubSubDriver
    {
        $port = $this->startServer($mode);

        $this->driver = new GcpPubSubDriver(ConnectionConfig::fromArray('protocol', [
            'driver' => 'gcp-pubsub',
            'project_id' => 'test-project',
            'emulator_host' => '127.0.0.1:' . $port,
            'timeout' => 2.0,
        ]));

        return $this->driver;
    }

    private function startServer(string $mode): int
    {
        $process = proc_open(
            [PHP_BINARY, self::FIXTURE, $mode],
            [
                0 => ['pipe', 'r'],
                1 => ['pipe', 'w'],
                2 => ['pipe', 'w'],
            ],
            $pipes,
        );
        self::assertIsResource($process);
        $this->process = $process;
        $this->pipes = $pipes;

        stream_set_timeout($pipes[1], 5);
        $line = fgets($pipes[1]);
        if ($line === false) {
            self::fail('Fake Pub/Sub server did not report its port: ' . (string) stream_get_contents($pipes[2]));
        }

        $port = (int) trim($line);
        self::assertGreaterThan(0, $port);

        return $port;
    }
}
