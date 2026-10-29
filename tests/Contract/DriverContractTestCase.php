<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Contract;

use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use PHPUnit\Framework\TestCase;

abstract class DriverContractTestCase extends TestCase
{
    private DriverInterface $driver;
    private string $destination;

    protected function setUp(): void
    {
        $this->driver = $this->createDriver();
        $this->destination = 'contract-' . bin2hex(random_bytes(6));
    }

    protected function tearDown(): void
    {
        $this->driver->close();
    }

    abstract protected function createDriver(): DriverInterface;

    public function test_publish_and_receive_roundtrip(): void
    {
        $this->subscribeIfNeeded($this->destination);

        $result = $this->driver->publish($this->destination, new Message('hello'));
        self::assertSame($this->destination, $result->destination);

        $message = $this->driver->receive($this->destination, new ReceiveOptions(100));
        self::assertNotNull($message);
        self::assertSame('hello', $message->body());

        // Brokers that assign an id only on delivery (e.g. NSQ) return null here.
        if ($result->id !== null) {
            self::assertSame($result->id, $message->id());
        }
    }

    public function test_ack_removes_message(): void
    {
        if (!$this->driver->capabilities()->ack) {
            self::markTestSkipped('Driver does not support acknowledgements.');
        }

        $this->subscribeIfNeeded($this->destination);
        $this->driver->publish($this->destination, new Message('ack-me'));
        $message = $this->driver->receive($this->destination, new ReceiveOptions(100));
        self::assertNotNull($message);
        $message->ack();

        self::assertNull($this->driver->receive($this->destination, new ReceiveOptions(10)));
    }

    public function test_reject_requeues_message(): void
    {
        if (!$this->driver->capabilities()->requeue) {
            self::markTestSkipped('Driver does not support requeue.');
        }

        $this->subscribeIfNeeded($this->destination);
        $this->driver->publish($this->destination, new Message('retry-me'));
        $first = $this->driver->receive($this->destination, new ReceiveOptions(100));
        self::assertNotNull($first);
        $first->reject(true);

        $second = $this->driver->receive($this->destination, new ReceiveOptions(100));
        self::assertNotNull($second);
        self::assertSame('retry-me', $second->body());
    }

    public function test_receive_returns_null_on_timeout(): void
    {
        self::assertNull($this->driver->receive($this->destination, new ReceiveOptions(10)));
    }

    public function test_headers_and_key_are_preserved(): void
    {
        if (!$this->driver->capabilities()->headers) {
            self::markTestSkipped('Driver does not support headers.');
        }

        $this->subscribeIfNeeded($this->destination);
        $this->driver->publish($this->destination, new Message(
            'payload',
            ['trace' => 'abc', 'attempt' => 2],
            'key-1',
        ));

        $message = $this->driver->receive($this->destination, new ReceiveOptions(100));
        self::assertNotNull($message);
        self::assertSame(
            $this->stringifyHeaders(['trace' => 'abc', 'attempt' => 2]),
            $this->stringifyHeaders($message->headers()),
        );
        self::assertSame('key-1', $message->key());
    }

    public function test_binary_body_is_preserved(): void
    {
        $body = "\x00\x01\xffbinary";
        $this->subscribeIfNeeded($this->destination);
        $this->driver->publish($this->destination, new Message($body));

        $message = $this->driver->receive($this->destination, new ReceiveOptions(100));
        self::assertNotNull($message);
        self::assertSame($body, $message->body());
    }

    /**
     * Fire-and-forget brokers only deliver to subscriptions created before the publish.
     */
    private function subscribeIfNeeded(string $destination): void
    {
        if ($this->driver->capabilities()->requiresActiveSubscription) {
            $this->driver->receive($destination, new ReceiveOptions(1));
        }
    }

    /**
     * Several broker protocols carry headers as strings; compare scalar values
     * by their string form so drivers need not invent types on the wire.
     *
     * @param array<string, mixed> $headers
     * @return array<string, mixed>
     */
    private function stringifyHeaders(array $headers): array
    {
        ksort($headers);

        return array_map(
            static fn (mixed $value): mixed => is_scalar($value) ? (string) $value : $value,
            $headers,
        );
    }

    public function test_batch_publish_when_supported(): void
    {
        if (!$this->driver->capabilities()->batchPublish) {
            self::markTestSkipped('Driver does not support batch publishing.');
        }

        $this->subscribeIfNeeded($this->destination);
        $results = $this->driver->publishBatch($this->destination, [
            new Message('one'),
            new Message('two'),
        ]);

        self::assertCount(2, $results);
        self::assertSame('one', $this->driver->receive($this->destination, new ReceiveOptions(100))?->body());
        self::assertSame('two', $this->driver->receive($this->destination, new ReceiveOptions(100))?->body());
    }
}
