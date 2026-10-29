<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Protocol\Azure;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Azure\ServiceBusFactory;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\RequestException;
use PHPUnit\Framework\TestCase;

final class ServiceBusProtocolTest extends TestCase
{
    private const FIXTURE = __DIR__ . '/fixtures/fake_servicebus_server.php';
    private const KEY_NAME = 'RootManageSharedAccessKey';
    private const KEY = 'dGVzdC1rZXktZm9yLXNlcnZpY2UtYnVz';

    /** @var resource|null */
    private $process = null;

    /** @var array<int, resource> */
    private array $pipes = [];

    private ?DriverInterface $driver = null;

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

    public function test_publish_receive_and_ack_roundtrip(): void
    {
        $driver = $this->brokerDriver();

        $result = $driver->publish('orders', new Message("\x00hello", ['trace' => 'abc', 'attempt' => 2], 'key-1'));
        self::assertSame('orders', $result->destination);
        self::assertNotNull($result->id);

        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);
        self::assertSame("\x00hello", $message->body());
        self::assertSame(['trace' => 'abc', 'attempt' => 2], $message->headers());
        self::assertSame('key-1', $message->key());
        self::assertSame($result->id, $message->id());
        self::assertSame(1, $message->attempts());

        $message->ack();
        self::assertNull($driver->receive('orders', new ReceiveOptions(10)));
    }

    public function test_reject_with_requeue_redelivers_the_message(): void
    {
        $driver = $this->brokerDriver();
        $driver->publish('orders', new Message('retry-me'));

        $first = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($first);
        $first->reject(true);

        $second = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($second);
        self::assertSame('retry-me', $second->body());
        self::assertSame(2, $second->attempts());
        $second->ack();
    }

    public function test_reject_without_requeue_discards_the_message(): void
    {
        $driver = $this->brokerDriver();
        $driver->publish('orders', new Message('drop-me'));

        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);
        $message->reject(false);

        self::assertNull($driver->receive('orders', new ReceiveOptions(10)));
    }

    public function test_delayed_message_is_not_visible_until_the_scheduled_time(): void
    {
        $driver = $this->brokerDriver();

        $driver->publish('orders', new Message('later', [], null, 1000));
        self::assertNull($driver->receive('orders', new ReceiveOptions(10)));

        usleep(1_100_000);

        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);
        self::assertSame('later', $message->body());
        $message->ack();
    }

    public function test_missing_entity_is_reported_as_a_request_error(): void
    {
        $driver = $this->brokerDriver();

        try {
            $driver->publish('missing-queue', new Message('hello'));
            self::fail('Expected a RequestException.');
        } catch (RequestException $exception) {
            self::assertSame(410, $exception->statusCode());
            self::assertStringContainsString('does not exist', $exception->getMessage());
        }
    }

    public function test_bad_credentials_are_reported_as_an_authentication_error(): void
    {
        $driver = $this->brokerDriver('not-the-key');

        $this->expectException(AuthenticationException::class);

        $driver->publish('orders', new Message('hello'));
    }

    public function test_completing_twice_reports_a_lost_lock(): void
    {
        $driver = $this->brokerDriver();
        $driver->publish('orders', new Message('hello'));

        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);
        $message->ack();

        try {
            $message->ack();
            self::fail('Expected a RequestException.');
        } catch (RequestException $exception) {
            self::assertSame(404, $exception->statusCode());
            self::assertStringContainsString('lock lost', $exception->getMessage());
        }
    }

    public function test_empty_queue_returns_null(): void
    {
        $driver = $this->brokerDriver();

        self::assertNull($driver->receive('orders', new ReceiveOptions(10)));
    }

    private function brokerDriver(?string $key = null): DriverInterface
    {
        $port = $this->startServer();

        $this->driver = (new ServiceBusFactory())->create(ConnectionConfig::fromArray('protocol', [
            'driver' => 'azure-service-bus',
            'endpoint' => 'http://127.0.0.1:' . $port,
            'queue' => 'orders',
            'shared_access_key_name' => self::KEY_NAME,
            'shared_access_key' => $key ?? self::KEY,
            'timeout' => 2.0,
        ]));

        return $this->driver;
    }

    private function startServer(): int
    {
        $process = proc_open(
            [PHP_BINARY, self::FIXTURE, self::KEY_NAME, self::KEY],
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
            self::fail('Fake Service Bus server did not report its port: ' . (string) stream_get_contents($pipes[2]));
        }

        $port = (int) trim($line);
        self::assertGreaterThan(0, $port);

        return $port;
    }
}
