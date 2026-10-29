<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Nats;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Driver\Nats\NatsDriver;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class NatsDriverTest extends TestCase
{
    public function test_capabilities_match_core_nats(): void
    {
        $capabilities = $this->driver()->capabilities();

        self::assertFalse($capabilities->ack);
        self::assertFalse($capabilities->requeue);
        self::assertFalse($capabilities->delay);
        self::assertFalse($capabilities->consumerGroups);
        self::assertFalse($capabilities->batchPublish);
        self::assertTrue($capabilities->ordering);
        self::assertTrue($capabilities->headers);
        self::assertTrue($capabilities->requiresActiveSubscription);
    }

    public function test_queue_group_enables_consumer_groups(): void
    {
        self::assertTrue($this->driver(['queue_group' => 'workers'])->capabilities()->consumerGroups);
    }

    public function test_headers_can_be_disabled(): void
    {
        self::assertFalse($this->driver(['headers' => false])->capabilities()->headers);
    }

    public function test_delayed_messages_are_rejected(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publish('orders', new Message('body', [], null, 10));
    }

    public function test_headers_are_rejected_when_disabled(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver(['headers' => false])->publish('orders', new Message('body', ['trace' => 'abc']));
    }

    public function test_keys_are_rejected_when_headers_are_disabled(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver(['headers' => false])->publish('orders', new Message('body', [], 'key-1'));
    }

    public function test_batch_publish_is_rejected(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publishBatch('orders', [new Message('one')]);
    }

    public function test_pass_requires_user(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(['pass' => 'secret']);
    }

    public function test_invalid_port_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(['port' => 'nope']);
    }

    public function test_invalid_timeout_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(['timeout' => 0]);
    }

    public function test_invalid_queue_group_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(['queue_group' => 'two words']);
    }

    /**
     * @param array<string, mixed> $options
     */
    private function driver(array $options = []): NatsDriver
    {
        return new NatsDriver(ConnectionConfig::fromArray('nats', $options + [
            'driver' => 'nats',
            'host' => '127.0.0.1',
        ]));
    }
}
