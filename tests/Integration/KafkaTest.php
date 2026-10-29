<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Integration;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Kafka\KafkaFactory;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Erikwang2013\Messaging\Tests\Contract\DriverContractTestCase;
use PHPUnit\Framework\Attributes\Group;

/**
 * The shared contract expects very short receive timeouts. Kafka's first group
 * join can take seconds, so every test uses a longer timeout and setUp joins
 * the group once before the timed assertions run.
 */
#[Group('integration')]
final class KafkaTest extends DriverContractTestCase
{
    private const RECEIVE_TIMEOUT_MS = 15000;

    private DriverInterface $driver;
    private string $topic;

    protected function setUp(): void
    {
        $this->driver = $this->createDriver();
        $this->topic = 'contract-' . bin2hex(random_bytes(6));

        try {
            // Long enough for Kafka's group.initial.rebalance.delay.ms (3s default),
            // short enough not to spend the full per-test timeout on an empty topic.
            $this->driver->receive($this->topic, new ReceiveOptions(5000));
        } catch (ConnectionException $exception) {
            self::markTestSkipped('Kafka is not available: ' . $exception->getMessage());
        } catch (ProtocolException $exception) {
            self::markTestSkipped('Kafka topic is not consumable (auto-create disabled?): ' . $exception->getMessage());
        } catch (UnsupportedFeatureException $exception) {
            self::markTestSkipped('Kafka broker version is not supported: ' . $exception->getMessage());
        }
    }

    protected function tearDown(): void
    {
        $this->driver->close();
    }

    protected function createDriver(): DriverInterface
    {
        $config = ConnectionConfig::fromArray('kafka', [
            'driver' => 'kafka',
            'brokers' => getenv('MESSAGING_KAFKA_BROKERS') ?: '127.0.0.1:9092',
            'group_id' => 'messaging-integration-' . bin2hex(random_bytes(4)),
            'client_id' => 'messaging-php-integration',
            'auto_offset_reset' => 'earliest',
            'timeout' => 10.0,
            'fetch_max_wait_ms' => 50,
            'session_timeout_ms' => 10000,
            'max_poll_records' => 10,
        ]);

        return (new KafkaFactory())->create($config);
    }

    public function test_publish_and_receive_roundtrip(): void
    {
        $result = $this->driver->publish($this->topic, new Message('hello'));
        self::assertSame($this->topic, $result->destination);
        self::assertNotNull($result->id);

        $message = $this->driver->receive($this->topic, new ReceiveOptions(self::RECEIVE_TIMEOUT_MS));
        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame($result->id, $message->id());
    }

    public function test_ack_removes_message(): void
    {
        $this->driver->publish($this->topic, new Message('ack-me'));

        $message = $this->driver->receive($this->topic, new ReceiveOptions(self::RECEIVE_TIMEOUT_MS));
        self::assertNotNull($message);
        $message->ack();

        self::assertNull($this->driver->receive($this->topic, new ReceiveOptions(1000)));
    }

    public function test_reject_requeues_message(): void
    {
        $this->driver->publish($this->topic, new Message('retry-me'));

        $first = $this->driver->receive($this->topic, new ReceiveOptions(self::RECEIVE_TIMEOUT_MS));
        self::assertNotNull($first);
        $first->reject(true);

        $second = $this->driver->receive($this->topic, new ReceiveOptions(self::RECEIVE_TIMEOUT_MS));
        self::assertNotNull($second);
        self::assertSame('retry-me', $second->body());
        $second->ack();
    }

    public function test_receive_returns_null_on_timeout(): void
    {
        self::assertNull($this->driver->receive($this->topic, new ReceiveOptions(1000)));
    }

    public function test_headers_and_key_are_preserved(): void
    {
        $this->driver->publish($this->topic, new Message(
            'payload',
            ['trace' => 'abc', 'attempt' => 2],
            'key-1',
        ));

        $message = $this->driver->receive($this->topic, new ReceiveOptions(self::RECEIVE_TIMEOUT_MS));
        self::assertNotNull($message);
        self::assertSame('abc', $message->headers()['trace']);
        self::assertSame('2', $message->headers()['attempt']);
        self::assertSame('key-1', $message->key());
    }

    public function test_binary_body_is_preserved(): void
    {
        $body = "\x00\x01\xffbinary";
        $this->driver->publish($this->topic, new Message($body));

        $message = $this->driver->receive($this->topic, new ReceiveOptions(self::RECEIVE_TIMEOUT_MS));
        self::assertNotNull($message);
        self::assertSame($body, $message->body());
    }

    public function test_batch_publish_when_supported(): void
    {
        $results = $this->driver->publishBatch($this->topic, [
            new Message('one'),
            new Message('two'),
        ]);

        self::assertCount(2, $results);
        self::assertSame(
            'one',
            $this->driver->receive($this->topic, new ReceiveOptions(self::RECEIVE_TIMEOUT_MS))?->body(),
        );
        self::assertSame(
            'two',
            $this->driver->receive($this->topic, new ReceiveOptions(self::RECEIVE_TIMEOUT_MS))?->body(),
        );
    }
}
