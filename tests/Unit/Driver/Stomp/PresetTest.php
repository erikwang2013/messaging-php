<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Stomp;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Driver\Stomp\ActiveMqFactory;
use Erikwang2013\Messaging\Driver\Stomp\ActiveMqPreset;
use Erikwang2013\Messaging\Driver\Stomp\ArtemisFactory;
use Erikwang2013\Messaging\Driver\Stomp\ArtemisPreset;
use Erikwang2013\Messaging\Driver\Stomp\StompDriver;
use PHPUnit\Framework\TestCase;

final class PresetTest extends TestCase
{
    public function test_active_mq_preset(): void
    {
        $preset = ActiveMqPreset::create();

        self::assertSame('activemq', $preset->name);
        self::assertSame(61613, $preset->defaultPort);
        self::assertTrue($preset->requeue);
        self::assertTrue($preset->delay);
        self::assertSame('AMQ_SCHEDULED_DELAY', $preset->delayHeader);
        self::assertFalse($preset->delayIsAbsolute);
        self::assertFalse($preset->requiresActiveSubscription);
        self::assertSame(10000, $preset->bufferLimit);
    }

    public function test_artemis_preset(): void
    {
        $preset = ArtemisPreset::create();

        self::assertSame('artemis', $preset->name);
        self::assertSame(61613, $preset->defaultPort);
        self::assertFalse($preset->requeue);
        self::assertTrue($preset->delay);
        self::assertSame('AMQ_SCHEDULED_DELAY', $preset->delayHeader);
        self::assertFalse($preset->delayIsAbsolute);
        self::assertTrue($preset->requiresActiveSubscription);
        self::assertSame(10000, $preset->bufferLimit);
    }

    public function test_active_mq_capabilities(): void
    {
        $capabilities = $this->factoryDriver(ActiveMqFactory::class, 'activemq')->capabilities();

        self::assertTrue($capabilities->ack);
        self::assertTrue($capabilities->requeue);
        self::assertTrue($capabilities->delay);
        self::assertFalse($capabilities->consumerGroups);
        self::assertFalse($capabilities->batchPublish);
        self::assertFalse($capabilities->ordering);
        self::assertTrue($capabilities->headers);
        self::assertFalse($capabilities->requiresActiveSubscription);
    }

    public function test_artemis_capabilities(): void
    {
        $capabilities = $this->factoryDriver(ArtemisFactory::class, 'artemis')->capabilities();

        self::assertTrue($capabilities->ack);
        self::assertFalse($capabilities->requeue);
        self::assertTrue($capabilities->delay);
        self::assertFalse($capabilities->consumerGroups);
        self::assertFalse($capabilities->batchPublish);
        self::assertFalse($capabilities->ordering);
        self::assertTrue($capabilities->headers);
        self::assertTrue($capabilities->requiresActiveSubscription);
    }

    public function test_missing_host_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new ActiveMqFactory())->create(ConnectionConfig::fromArray('amq', ['driver' => 'activemq']));
    }

    public function test_invalid_port_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new ActiveMqFactory())->create(ConnectionConfig::fromArray('amq', [
            'driver' => 'activemq',
            'host' => '127.0.0.1',
            'port' => 70000,
        ]));
    }

    public function test_invalid_timeout_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new ActiveMqFactory())->create(ConnectionConfig::fromArray('amq', [
            'driver' => 'activemq',
            'host' => '127.0.0.1',
            'timeout' => 0,
        ]));
    }

    public function test_invalid_buffer_limit_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new ActiveMqFactory())->create(ConnectionConfig::fromArray('amq', [
            'driver' => 'activemq',
            'host' => '127.0.0.1',
            'buffer_limit' => 0,
        ]));
    }

    /**
     * @param class-string<ActiveMqFactory|ArtemisFactory> $factory
     */
    private function factoryDriver(string $factory, string $driver): StompDriver
    {
        $config = ConnectionConfig::fromArray($driver, [
            'driver' => $driver,
            'host' => '127.0.0.1',
        ]);

        $created = (new $factory())->create($config);
        self::assertInstanceOf(StompDriver::class, $created);

        return $created;
    }
}
