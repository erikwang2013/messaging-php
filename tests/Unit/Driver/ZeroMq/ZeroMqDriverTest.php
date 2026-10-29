<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\ZeroMq;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\ZeroMq\ZeroMqDriver;
use Erikwang2013\Messaging\Driver\ZeroMq\ZeroMqFactory;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class ZeroMqDriverTest extends TestCase
{
    public function test_pubsub_capabilities(): void
    {
        $capabilities = $this->driver()->capabilities();

        self::assertFalse($capabilities->ack);
        self::assertFalse($capabilities->requeue);
        self::assertFalse($capabilities->delay);
        self::assertFalse($capabilities->consumerGroups);
        self::assertFalse($capabilities->batchPublish);
        self::assertTrue($capabilities->ordering);
        self::assertFalse($capabilities->headers);
        self::assertTrue($capabilities->requiresActiveSubscription);
    }

    public function test_pushpull_capabilities_do_not_require_subscription(): void
    {
        $capabilities = $this->driver(['pattern' => 'pushpull'])->capabilities();

        self::assertTrue($capabilities->ordering);
        self::assertFalse($capabilities->requiresActiveSubscription);
    }

    public function test_headers_are_rejected(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publish('orders', new Message('body', ['trace' => 'abc']));
    }

    public function test_keys_are_rejected(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publish('orders', new Message('body', [], 'key-1'));
    }

    public function test_delayed_messages_are_rejected(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publish('orders', new Message('body', [], null, 10));
    }

    public function test_batch_publish_is_rejected(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publishBatch('orders', [new Message('one')]);
    }

    public function test_empty_publish_destination_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver()->publish('', new Message('body'));
    }

    public function test_empty_receive_destination_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver()->receive('', new ReceiveOptions(1));
    }

    public function test_port_is_required(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        new ZeroMqDriver(ConnectionConfig::fromArray('zeromq', [
            'driver' => 'zeromq',
            'host' => '127.0.0.1',
        ]));
    }

    public function test_invalid_port_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(['port' => 'nope']);
    }

    public function test_port_out_of_range_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(['port' => 70000]);
    }

    public function test_invalid_pattern_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(['pattern' => 'reqrep']);
    }

    public function test_invalid_timeout_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(['timeout' => 0]);
    }

    public function test_invalid_identity_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(['identity' => '']);
    }

    public function test_invalid_tcp_nodelay_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(['tcp_nodelay' => 'yes']);
    }

    public function test_non_positive_buffer_limit_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(['buffer_limit' => 0]);
    }

    public function test_non_integer_buffer_limit_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(['buffer_limit' => '10']);
    }

    public function test_factory_does_not_connect_eagerly(): void
    {
        $driver = (new ZeroMqFactory())->create(ConnectionConfig::fromArray('zeromq', [
            'driver' => 'zeromq',
            'host' => '127.0.0.1',
            'port' => 1,
        ]));

        self::assertInstanceOf(DriverInterface::class, $driver);
        self::assertTrue($driver->capabilities()->requiresActiveSubscription);
    }

    public function test_close_is_idempotent(): void
    {
        $driver = $this->driver();

        $driver->close();
        $driver->close();

        $this->expectNotToPerformAssertions();
    }

    /**
     * @param array<string, mixed> $options
     */
    private function driver(array $options = []): ZeroMqDriver
    {
        return new ZeroMqDriver(ConnectionConfig::fromArray('zeromq', $options + [
            'driver' => 'zeromq',
            'host' => '127.0.0.1',
            'port' => 5555,
        ]));
    }
}
