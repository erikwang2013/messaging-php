<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\RabbitMq;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\RabbitMq\RabbitMqDriver;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class RabbitMqDriverTest extends TestCase
{
    public function test_capabilities(): void
    {
        $capabilities = $this->driver()->capabilities();

        self::assertTrue($capabilities->ack);
        self::assertTrue($capabilities->requeue);
        self::assertFalse($capabilities->delay);
        self::assertFalse($capabilities->consumerGroups);
        self::assertFalse($capabilities->batchPublish);
        self::assertFalse($capabilities->ordering);
        self::assertTrue($capabilities->headers);
        self::assertFalse($capabilities->requiresActiveSubscription);
    }

    public function test_batch_publish_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publishBatch('orders', [new Message('one')]);
    }

    public function test_delay_is_unsupported_before_connecting(): void
    {
        $this->expectException(UnsupportedFeatureException::class);
        $this->expectExceptionMessage('delayed_message_exchange');

        $this->driver()->publish('orders', new Message('later', [], null, 10));
    }

    public function test_empty_destination_is_rejected_before_connecting(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver()->publish('', new Message('nope'));
    }

    public function test_overlong_destination_is_rejected_before_connecting(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver()->receive(str_repeat('q', 256), new ReceiveOptions(1));
    }

    public function test_host_is_required(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('host');

        new RabbitMqDriver(ConnectionConfig::fromArray('rabbitmq', ['driver' => 'rabbitmq']));
    }

    public function test_invalid_prefetch_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('prefetch');

        new RabbitMqDriver($this->config(['prefetch' => -1]));
    }

    public function test_invalid_heartbeat_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('heartbeat');

        new RabbitMqDriver($this->config(['heartbeat' => 70000]));
    }

    public function test_invalid_tls_context_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('tls_context');

        new RabbitMqDriver($this->config(['tls_context' => 'nope']));
    }

    public function test_invalid_timeout_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('timeout');

        new RabbitMqDriver($this->config(['timeout' => 0]));
    }

    private function driver(): RabbitMqDriver
    {
        return new RabbitMqDriver($this->config());
    }

    /**
     * @param array<string, mixed> $options
     */
    private function config(array $options = []): ConnectionConfig
    {
        return ConnectionConfig::fromArray('rabbitmq', array_merge([
            'driver' => 'rabbitmq',
            'host' => '127.0.0.1',
        ], $options));
    }
}
