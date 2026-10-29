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
        $result = $this->driver->publish($this->destination, new Message('hello'));
        self::assertSame($this->destination, $result->destination);
        self::assertNotNull($result->id);

        $message = $this->driver->receive($this->destination, new ReceiveOptions(100));
        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame($result->id, $message->id());
    }

    public function test_ack_removes_message(): void
    {
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

        $this->driver->publish($this->destination, new Message(
            'payload',
            ['trace' => 'abc', 'attempt' => 2],
            'key-1',
        ));

        $message = $this->driver->receive($this->destination, new ReceiveOptions(100));
        self::assertNotNull($message);
        self::assertSame(['trace' => 'abc', 'attempt' => 2], $message->headers());
        self::assertSame('key-1', $message->key());
    }

    public function test_binary_body_is_preserved(): void
    {
        $body = "\x00\x01\xffbinary";
        $this->driver->publish($this->destination, new Message($body));

        $message = $this->driver->receive($this->destination, new ReceiveOptions(100));
        self::assertNotNull($message);
        self::assertSame($body, $message->body());
    }

    public function test_batch_publish_when_supported(): void
    {
        if (!$this->driver->capabilities()->batchPublish) {
            self::markTestSkipped('Driver does not support batch publishing.');
        }

        $results = $this->driver->publishBatch($this->destination, [
            new Message('one'),
            new Message('two'),
        ]);

        self::assertCount(2, $results);
        self::assertSame('one', $this->driver->receive($this->destination, new ReceiveOptions(100))?->body());
        self::assertSame('two', $this->driver->receive($this->destination, new ReceiveOptions(100))?->body());
    }
}
