<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Integration;

use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\RequestException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Erikwang2013\Messaging\Tests\Protocol\Aws\AwsTestSupport;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

#[Group('integration')]
final class SnsTest extends TestCase
{
    private ?DriverInterface $driver = null;
    private string $topicArn;

    protected function setUp(): void
    {
        $name = 'contract-' . bin2hex(random_bytes(6));

        try {
            $this->topicArn = AwsTestSupport::createTopic($name);
        } catch (ConnectionException|TimeoutException $exception) {
            self::markTestSkipped('AWS-compatible endpoint is not available: ' . $exception->getMessage());
        }

        $this->driver = AwsTestSupport::driver('sns');
    }

    protected function tearDown(): void
    {
        $this->driver?->close();

        try {
            AwsTestSupport::deleteTopic($this->topicArn);
        } catch (\Throwable) {
            // Best-effort cleanup against shared test infrastructure.
        }
    }

    public function test_capabilities(): void
    {
        $capabilities = $this->requireDriver()->capabilities();

        self::assertFalse($capabilities->ack);
        self::assertFalse($capabilities->requeue);
        self::assertFalse($capabilities->delay);
        self::assertFalse($capabilities->batchPublish);
        self::assertFalse($capabilities->consumerGroups);
        self::assertFalse($capabilities->ordering);
        self::assertTrue($capabilities->headers);
    }

    public function test_publish_returns_a_message_id(): void
    {
        $result = $this->requireDriver()->publish($this->topicArn, new Message('hello', ['trace' => 'abc']));

        self::assertSame($this->topicArn, $result->destination);
        self::assertNotNull($result->id);
        self::assertSame($result->id, $result->raw['MessageId']);
    }

    public function test_publish_fans_out_to_a_subscribed_sqs_queue(): void
    {
        $queueName = 'contract-' . bin2hex(random_bytes(6));
        $queueUrl = AwsTestSupport::createQueue($queueName);
        $sqs = AwsTestSupport::driver('sqs');

        try {
            AwsTestSupport::subscribeQueue($this->topicArn, AwsTestSupport::queueArn($queueUrl));

            $result = $this->requireDriver()->publish($this->topicArn, new Message('hello'));
            self::assertNotNull($result->id);

            $deadline = microtime(true) + 10.0;
            $message = null;
            while ($message === null && microtime(true) < $deadline) {
                $message = $sqs->receive($queueUrl, new ReceiveOptions(500));
            }

            self::assertNotNull($message);
            self::assertSame('hello', $message->body());
            $message->ack();
        } finally {
            $sqs->close();
            AwsTestSupport::deleteQueue($queueUrl);
        }
    }

    public function test_publish_to_a_missing_topic_fails(): void
    {
        $arn = 'arn:aws:sns:us-east-1:000000000000:missing-' . bin2hex(random_bytes(6));

        $this->expectException(RequestException::class);

        $this->requireDriver()->publish($arn, new Message('hello'));
    }

    public function test_receive_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->requireDriver()->receive($this->topicArn, new ReceiveOptions(10));
    }

    public function test_batch_publish_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->requireDriver()->publishBatch($this->topicArn, [new Message('x')]);
    }

    public function test_key_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->requireDriver()->publish($this->topicArn, new Message('x', [], 'key'));
    }

    public function test_delay_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->requireDriver()->publish($this->topicArn, new Message('x', [], null, 100));
    }

    private function requireDriver(): DriverInterface
    {
        self::assertNotNull($this->driver);

        return $this->driver;
    }
}
