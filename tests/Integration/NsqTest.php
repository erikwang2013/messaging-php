<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Integration;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Nsq\NsqFactory;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Erikwang2013\Messaging\Tests\Contract\DriverContractTestCase;
use PHPUnit\Framework\Attributes\Group;

#[Group('integration')]
final class NsqTest extends DriverContractTestCase
{
    private DriverInterface $driver;
    private string $topic;

    protected function setUp(): void
    {
        $this->driver = $this->createDriver();
        $this->topic = 'contract-' . bin2hex(random_bytes(6));

        try {
            // A channel only receives messages published after SUB, so create it before publishing.
            $this->driver->receive($this->topic, new ReceiveOptions(20));
        } catch (ConnectionException $exception) {
            self::markTestSkipped('NSQ is not available: ' . $exception->getMessage());
        }
    }

    protected function tearDown(): void
    {
        $this->driver->close();
    }

    protected function createDriver(): DriverInterface
    {
        $config = ConnectionConfig::fromArray('nsq', [
            'driver' => 'nsq',
            'host' => getenv('MESSAGING_NSQD_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('MESSAGING_NSQD_PORT') ?: 4150),
            'channel' => 'default',
            'timeout' => 2.0,
        ]);

        return (new NsqFactory())->create($config);
    }

    public function test_publish_and_receive_roundtrip(): void
    {
        $result = $this->driver->publish($this->topic, new Message('hello'));

        self::assertSame($this->topic, $result->destination);
        self::assertNull($result->id);

        $message = $this->driver->receive($this->topic, new ReceiveOptions(2000));
        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertNotNull($message->id());
        self::assertSame(1, $message->attempts());
        $message->ack();
    }

    public function test_ack_removes_message(): void
    {
        $this->driver->publish($this->topic, new Message('ack-me'));
        $message = $this->driver->receive($this->topic, new ReceiveOptions(2000));
        self::assertNotNull($message);
        $message->ack();

        self::assertNull($this->driver->receive($this->topic, new ReceiveOptions(100)));
    }

    public function test_reject_requeues_message(): void
    {
        $this->driver->publish($this->topic, new Message('retry-me'));
        $first = $this->driver->receive($this->topic, new ReceiveOptions(2000));
        self::assertNotNull($first);
        $first->reject(true);

        $second = $this->driver->receive($this->topic, new ReceiveOptions(2000));
        self::assertNotNull($second);
        self::assertSame('retry-me', $second->body());
        self::assertSame(2, $second->attempts());
        $second->ack();
    }

    public function test_receive_returns_null_on_timeout(): void
    {
        self::assertNull($this->driver->receive($this->topic, new ReceiveOptions(20)));
    }

    public function test_headers_and_key_are_preserved(): void
    {
        self::assertFalse($this->driver->capabilities()->headers);

        $this->expectException(UnsupportedFeatureException::class);
        $this->driver->publish($this->topic, new Message('payload', ['trace' => 'abc'], 'key-1'));
    }

    public function test_publish_delay_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver->publish($this->topic, new Message('payload', [], null, 100));
    }

    public function test_binary_body_is_preserved(): void
    {
        $body = "\x00\x01\xffbinary";
        $this->driver->publish($this->topic, new Message($body));

        $message = $this->driver->receive($this->topic, new ReceiveOptions(2000));
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
}
