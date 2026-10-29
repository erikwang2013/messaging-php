<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Integration;

use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Tests\Contract\DriverContractTestCase;
use Erikwang2013\Messaging\Tests\Protocol\Aws\AwsTestSupport;
use PHPUnit\Framework\Attributes\Group;

#[Group('integration')]
final class SqsTest extends DriverContractTestCase
{
    private ?DriverInterface $driver = null;
    private string $queueName;
    private string $queueUrl;

    protected function setUp(): void
    {
        $this->queueName = 'contract-' . bin2hex(random_bytes(6));

        try {
            $this->queueUrl = AwsTestSupport::createQueue($this->queueName);
        } catch (ConnectionException|TimeoutException $exception) {
            self::markTestSkipped('LocalStack is not available: ' . $exception->getMessage());
        }

        $this->driver = $this->createDriver();
    }

    protected function tearDown(): void
    {
        $this->driver?->close();

        try {
            AwsTestSupport::deleteQueue($this->queueUrl);
        } catch (\Throwable) {
            // Best-effort cleanup against shared test infrastructure.
        }
    }

    protected function createDriver(): DriverInterface
    {
        return AwsTestSupport::driver('sqs');
    }

    public function test_capabilities(): void
    {
        $capabilities = $this->requireDriver()->capabilities();

        self::assertTrue($capabilities->ack);
        self::assertTrue($capabilities->requeue);
        self::assertTrue($capabilities->delay);
        self::assertTrue($capabilities->batchPublish);
        self::assertTrue($capabilities->headers);
        self::assertFalse($capabilities->consumerGroups);
        self::assertFalse($capabilities->ordering);
    }

    public function test_publish_and_receive_roundtrip(): void
    {
        $driver = $this->requireDriver();

        $result = $driver->publish($this->queueUrl, new Message('hello'));

        self::assertSame($this->queueUrl, $result->destination);
        self::assertNotNull($result->id);

        $message = $driver->receive($this->queueUrl, new ReceiveOptions(2000));
        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame($result->id, $message->id());
        self::assertSame(1, $message->attempts());
        $message->ack();
    }

    public function test_ack_removes_message(): void
    {
        $driver = $this->requireDriver();

        $driver->publish($this->queueUrl, new Message('ack-me'));
        $message = $driver->receive($this->queueUrl, new ReceiveOptions(2000));
        self::assertNotNull($message);
        $message->ack();

        self::assertNull($driver->receive($this->queueUrl, new ReceiveOptions(1000)));
    }

    public function test_reject_requeues_message(): void
    {
        $driver = $this->requireDriver();

        $driver->publish($this->queueUrl, new Message('retry-me'));
        $first = $driver->receive($this->queueUrl, new ReceiveOptions(2000));
        self::assertNotNull($first);
        $first->reject(true);

        $second = $driver->receive($this->queueUrl, new ReceiveOptions(2000));
        self::assertNotNull($second);
        self::assertSame('retry-me', $second->body());
        self::assertGreaterThanOrEqual(2, $second->attempts());
        $second->ack();
    }

    public function test_receive_returns_null_on_timeout(): void
    {
        self::assertNull($this->requireDriver()->receive($this->queueUrl, new ReceiveOptions(200)));
    }

    public function test_headers_and_key_are_preserved(): void
    {
        $driver = $this->requireDriver();
        $name = 'contract-' . bin2hex(random_bytes(6)) . '.fifo';
        $url = AwsTestSupport::createQueue($name, true);

        try {
            $driver->publish($url, new Message('payload', ['trace' => 'abc', 'attempt' => 2], 'key-1'));

            $message = $driver->receive($url, new ReceiveOptions(2000));
            self::assertNotNull($message);

            // LocalStack/Moto do not always expose MessageGroupId on FIFO
            // receives; against real AWS this assertion still runs.
            if ($message->key() === null && getenv('MESSAGING_AWS_ENDPOINT') !== false) {
                self::markTestSkipped('The configured SQS mock does not expose MessageGroupId.');
            }

            self::assertSame('key-1', $message->key());
            self::assertSame(
                ['attempt' => '2', 'trace' => 'abc'],
                $this->stringifyHeaders($message->headers()),
            );
            $message->ack();
        } finally {
            AwsTestSupport::deleteQueue($url);
        }
    }

    public function test_binary_body_is_preserved(): void
    {
        self::markTestSkipped('SQS message bodies are UTF-8 strings; binary payloads are not supported.');
    }

    public function test_batch_publish_when_supported(): void
    {
        $driver = $this->requireDriver();

        $results = $driver->publishBatch($this->queueUrl, [
            new Message('one'),
            new Message('two'),
        ]);

        self::assertCount(2, $results);
        self::assertNotNull($results[0]->id);
        self::assertNotNull($results[1]->id);

        $bodies = [];
        for ($i = 0; $i < 2; $i++) {
            $message = $driver->receive($this->queueUrl, new ReceiveOptions(2000));
            self::assertNotNull($message);
            $bodies[] = $message->body();
            $message->ack();
        }

        sort($bodies);
        self::assertSame(['one', 'two'], $bodies);
    }

    public function test_delay_defers_delivery(): void
    {
        $driver = $this->requireDriver();

        $driver->publish($this->queueUrl, new Message('later', [], null, 500));

        self::assertNull($driver->receive($this->queueUrl, new ReceiveOptions(100)));

        $message = $driver->receive($this->queueUrl, new ReceiveOptions(3000));
        self::assertNotNull($message);
        self::assertSame('later', $message->body());
        $message->ack();
    }

    public function test_queue_name_destination_is_resolved(): void
    {
        $driver = $this->requireDriver();

        $result = $driver->publish($this->queueName, new Message('by-name'));
        self::assertSame($this->queueName, $result->destination);
        self::assertNotNull($result->id);

        $message = $driver->receive($this->queueName, new ReceiveOptions(2000));
        self::assertNotNull($message);
        self::assertSame('by-name', $message->body());
        $message->ack();
    }

    private function requireDriver(): DriverInterface
    {
        self::assertNotNull($this->driver);

        return $this->driver;
    }

    /**
     * @param array<string, mixed> $headers
     * @return array<string, mixed>
     */
    private function stringifyHeaders(array $headers): array
    {
        ksort($headers);

        return array_map(static fn (mixed $value): mixed => is_scalar($value) ? (string) $value : $value, $headers);
    }
}
