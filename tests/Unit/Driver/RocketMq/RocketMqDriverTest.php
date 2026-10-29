<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\RocketMq;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Driver\RocketMq\RocketMqDriver;
use Erikwang2013\Messaging\Driver\RocketMq\RocketMqFactory;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class RocketMqDriverTest extends TestCase
{
    public function test_capabilities(): void
    {
        $capabilities = $this->driver()->capabilities();

        self::assertTrue($capabilities->ack);
        self::assertTrue($capabilities->requeue);
        self::assertTrue($capabilities->delay);
        self::assertTrue($capabilities->consumerGroups);
        self::assertFalse($capabilities->batchPublish);
        self::assertTrue($capabilities->ordering);
        self::assertTrue($capabilities->headers);
        self::assertFalse($capabilities->requiresActiveSubscription);
    }

    public function test_batch_publish_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publishBatch('orders', [new Message('one')]);
    }

    public function test_invalid_destinations_are_rejected_before_connecting(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver()->publish('bad topic', new Message('one'));
    }

    public function test_close_is_idempotent(): void
    {
        $driver = $this->driver();
        $driver->close();
        $driver->close();

        self::assertTrue($driver->capabilities()->ack);
    }

    private function driver(): RocketMqDriver
    {
        $config = ConnectionConfig::fromArray('rocketmq', [
            'driver' => 'rocketmq',
            'nameserver' => '127.0.0.1:9876',
        ]);

        $driver = (new RocketMqFactory())->create($config);
        self::assertInstanceOf(RocketMqDriver::class, $driver);

        return $driver;
    }
}
