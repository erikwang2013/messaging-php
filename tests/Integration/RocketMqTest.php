<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Integration;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\RocketMq\RocketMqConfig;
use Erikwang2013\Messaging\Driver\RocketMq\RocketMqFactory;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Erikwang2013\Messaging\Support\Socket;
use Erikwang2013\Messaging\Tests\Contract\DriverContractTestCase;
use PHPUnit\Framework\Attributes\Group;

#[Group('integration')]
final class RocketMqTest extends DriverContractTestCase
{
    private DriverInterface $driver;
    private string $topic;
    private string $nameserver;

    protected function setUp(): void
    {
        $this->nameserver = getenv('MESSAGING_ROCKETMQ_NAMESERVER') ?: '127.0.0.1:9876';

        try {
            [$host, $port] = RocketMqConfig::parseAddress($this->nameserver, RocketMqConfig::DEFAULT_PORT);
            Socket::connect($host, $port, 1.0)->close();
        } catch (ConnectionException $exception) {
            self::markTestSkipped('RocketMQ nameserver is not available: ' . $exception->getMessage());
        }

        $this->driver = $this->createDriver();
        $this->topic = 'contract-' . bin2hex(random_bytes(6));
    }

    protected function tearDown(): void
    {
        $this->driver->close();
    }

    protected function createDriver(): DriverInterface
    {
        $config = ConnectionConfig::fromArray('rocketmq', [
            'driver' => 'rocketmq',
            'nameserver' => $this->nameserver,
            'group' => getenv('MESSAGING_ROCKETMQ_GROUP')
                ?: 'messaging-contract-' . bin2hex(random_bytes(4)),
            'timeout' => 3.0,
            'consume_from' => 'first',
        ]);

        return (new RocketMqFactory())->create($config);
    }

    public function test_capabilities(): void
    {
        $capabilities = $this->driver->capabilities();

        self::assertTrue($capabilities->ack);
        self::assertTrue($capabilities->requeue);
        self::assertTrue($capabilities->delay);
        self::assertTrue($capabilities->consumerGroups);
        self::assertFalse($capabilities->batchPublish);
        self::assertTrue($capabilities->ordering);
        self::assertTrue($capabilities->headers);
        self::assertFalse($capabilities->requiresActiveSubscription);
    }

    public function test_publish_and_receive_roundtrip(): void
    {
        $result = $this->driver->publish($this->topic, new Message('hello'));

        self::assertSame($this->topic, $result->destination);
        self::assertNotNull($result->id);

        $message = $this->driver->receive($this->topic, new ReceiveOptions(5000));
        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame($result->id, $message->id());
        self::assertSame(1, $message->attempts());
        $message->ack();
    }

    public function test_ack_removes_message(): void
    {
        $this->driver->publish($this->topic, new Message('ack-me'));
        $message = $this->driver->receive($this->topic, new ReceiveOptions(5000));
        self::assertNotNull($message);
        $message->ack();

        self::assertNull($this->driver->receive($this->topic, new ReceiveOptions(500)));
    }

    public function test_reject_requeues_message(): void
    {
        $this->driver->publish($this->topic, new Message('retry-me'));
        $first = $this->driver->receive($this->topic, new ReceiveOptions(5000));
        self::assertNotNull($first);
        $first->reject(true);

        // The broker always routes send-back through the retry topic with at least a 1s delay.
        $second = $this->driver->receive($this->topic, new ReceiveOptions(10000));
        self::assertNotNull($second);
        self::assertSame('retry-me', $second->body());
        self::assertSame(2, $second->attempts());
        $second->ack();
    }

    public function test_receive_returns_null_on_timeout(): void
    {
        self::assertNull($this->driver->receive($this->topic, new ReceiveOptions(300)));
    }

    public function test_headers_and_key_are_preserved(): void
    {
        $this->driver->publish($this->topic, new Message(
            'payload',
            ['trace' => 'abc', 'attempt' => 2],
            'key-1',
        ));

        $message = $this->driver->receive($this->topic, new ReceiveOptions(5000));
        self::assertNotNull($message);
        self::assertSame(
            $this->stringifyHeaders(['trace' => 'abc', 'attempt' => 2]),
            $this->stringifyHeaders($message->headers()),
        );
        self::assertSame('key-1', $message->key());
        $message->ack();
    }

    public function test_binary_body_is_preserved(): void
    {
        $body = "\x00\x01\xffbinary";
        $this->driver->publish($this->topic, new Message($body));

        $message = $this->driver->receive($this->topic, new ReceiveOptions(5000));
        self::assertNotNull($message);
        self::assertSame($body, $message->body());
        $message->ack();
    }

    public function test_batch_publish_when_supported(): void
    {
        self::assertFalse($this->driver->capabilities()->batchPublish);

        $this->expectException(UnsupportedFeatureException::class);
        $this->driver->publishBatch($this->topic, [new Message('one')]);
    }

    public function test_delay_defers_delivery(): void
    {
        $this->driver->publish($this->topic, new Message('later', [], null, 1000));

        self::assertNull($this->driver->receive($this->topic, new ReceiveOptions(300)));

        $message = $this->driver->receive($this->topic, new ReceiveOptions(10000));
        self::assertNotNull($message);
        self::assertSame('later', $message->body());
        $message->ack();
    }

    /**
     * @param array<string, mixed> $headers
     * @return array<string, mixed>
     */
    private function stringifyHeaders(array $headers): array
    {
        ksort($headers);

        return array_map(fn (mixed $value): mixed => is_scalar($value) ? (string) $value : $value, $headers);
    }
}
