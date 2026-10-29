<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Integration;

use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Erikwang2013\Messaging\Tests\Contract\DriverContractTestCase;
use Erikwang2013\Messaging\Tests\Protocol\Aws\AwsTestSupport;
use PHPUnit\Framework\Attributes\Group;

#[Group('integration')]
final class KinesisTest extends DriverContractTestCase
{
    private ?DriverInterface $driver = null;
    private string $streamName;

    protected function setUp(): void
    {
        $this->streamName = 'contract-' . bin2hex(random_bytes(6));

        try {
            AwsTestSupport::createStream($this->streamName);
            AwsTestSupport::waitForStream($this->streamName);
        } catch (ConnectionException|TimeoutException $exception) {
            self::markTestSkipped('LocalStack is not available: ' . $exception->getMessage());
        }

        $this->driver = $this->createDriver();
    }

    protected function tearDown(): void
    {
        $this->driver?->close();

        try {
            AwsTestSupport::deleteStream($this->streamName);
        } catch (\Throwable) {
            // Best-effort cleanup against shared test infrastructure.
        }
    }

    protected function createDriver(): DriverInterface
    {
        return AwsTestSupport::driver('kinesis');
    }

    public function test_capabilities(): void
    {
        $capabilities = $this->requireDriver()->capabilities();

        self::assertFalse($capabilities->ack);
        self::assertFalse($capabilities->requeue);
        self::assertFalse($capabilities->delay);
        self::assertFalse($capabilities->consumerGroups);
        self::assertTrue($capabilities->batchPublish);
        self::assertTrue($capabilities->ordering);
        self::assertFalse($capabilities->headers);
    }

    public function test_publish_and_receive_roundtrip(): void
    {
        $driver = $this->requireDriver();

        $result = $driver->publish($this->streamName, new Message('hello'));

        self::assertSame($this->streamName, $result->destination);
        self::assertNotNull($result->id);

        $message = $driver->receive($this->streamName, new ReceiveOptions(5000));
        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame($result->id, $message->id());
        self::assertSame(1, $message->attempts());
        $message->ack();
    }

    public function test_ack_removes_message(): void
    {
        self::markTestSkipped('Kinesis has no acknowledgements; ack() only advances an in-memory checkpoint.');
    }

    public function test_reject_requeues_message(): void
    {
        self::markTestSkipped('Kinesis has no requeue; consume from the checkpointed iterator instead.');
    }

    public function test_receive_returns_null_on_timeout(): void
    {
        self::assertNull($this->requireDriver()->receive($this->streamName, new ReceiveOptions(300)));
    }

    public function test_headers_and_key_are_preserved(): void
    {
        $driver = $this->requireDriver();

        $driver->publish($this->streamName, new Message('payload', [], 'key-1'));

        $message = $driver->receive($this->streamName, new ReceiveOptions(5000));
        self::assertNotNull($message);
        self::assertSame('key-1', $message->key());
        $message->ack();
    }

    public function test_binary_body_is_preserved(): void
    {
        $driver = $this->requireDriver();
        $body = "\x00\x01\xffbinary";

        $driver->publish($this->streamName, new Message($body));

        $message = $driver->receive($this->streamName, new ReceiveOptions(5000));
        self::assertNotNull($message);
        self::assertSame($body, $message->body());
        $message->ack();
    }

    public function test_batch_publish_when_supported(): void
    {
        $driver = $this->requireDriver();

        $results = $driver->publishBatch($this->streamName, [
            new Message('one', [], 'pk-1'),
            new Message('two', [], 'pk-1'),
        ]);

        self::assertCount(2, $results);
        self::assertNotNull($results[0]->id);
        self::assertNotNull($results[1]->id);

        $first = $driver->receive($this->streamName, new ReceiveOptions(5000));
        self::assertNotNull($first);
        self::assertSame('one', $first->body());
        $first->ack();

        $second = $driver->receive($this->streamName, new ReceiveOptions(5000));
        self::assertNotNull($second);
        self::assertSame('two', $second->body());
        $second->ack();
    }

    public function test_headers_are_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->requireDriver()->publish($this->streamName, new Message('payload', ['trace' => 'abc']));
    }

    public function test_delay_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->requireDriver()->publish($this->streamName, new Message('payload', [], null, 100));
    }

    public function test_checkpoint_callback_receives_the_acknowledged_sequence(): void
    {
        $checkpoints = [];
        $driver = AwsTestSupport::driver('kinesis', [
            'checkpoint' => function (string $stream, string $shard, string $sequence) use (&$checkpoints): void {
                $checkpoints[] = [$stream, $shard, $sequence];
            },
        ]);

        try {
            $result = $driver->publish($this->streamName, new Message('checkpoint-me'));
            self::assertNotNull($result->id);

            $message = $driver->receive($this->streamName, new ReceiveOptions(5000));
            self::assertNotNull($message);
            $message->ack();

            self::assertCount(1, $checkpoints);
            self::assertSame($this->streamName, $checkpoints[0][0]);
            self::assertStringStartsWith('shardId-', $checkpoints[0][1]);
            self::assertSame($result->id, $checkpoints[0][2]);
        } finally {
            $driver->close();
        }
    }

    private function requireDriver(): DriverInterface
    {
        self::assertNotNull($this->driver);

        return $this->driver;
    }
}
