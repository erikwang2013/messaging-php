<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Pulsar;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Driver\Pulsar\PulsarDriver;
use Erikwang2013\Messaging\Driver\Pulsar\PulsarFactory;
use PHPUnit\Framework\TestCase;

final class PulsarFactoryTest extends TestCase
{
    public function test_create_builds_a_driver_from_minimal_config(): void
    {
        $driver = (new PulsarFactory())->create($this->config(['service_url' => 'pulsar://127.0.0.1:6650']));

        self::assertInstanceOf(PulsarDriver::class, $driver);
    }

    public function test_create_accepts_every_documented_option(): void
    {
        $driver = (new PulsarFactory())->create($this->config([
            'service_url' => 'pulsar+ssl://127.0.0.1:6651',
            'topic' => 'persistent://public/default/events',
            'subscription' => 'workers',
            'consumer_name' => 'worker-1',
            'timeout' => 2.5,
            'tls' => true,
            'tls_context' => ['verify_peer' => false],
        ]));

        self::assertInstanceOf(PulsarDriver::class, $driver);
    }

    public function test_service_url_is_required(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new PulsarFactory())->create($this->config([]));
    }

    public function test_service_url_must_be_a_pulsar_url(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new PulsarFactory())->create($this->config(['service_url' => 'nats://127.0.0.1:4222']));
    }

    public function test_tls_must_be_a_boolean(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new PulsarFactory())->create($this->config([
            'service_url' => 'pulsar://127.0.0.1:6650',
            'tls' => 'yes',
        ]));
    }

    public function test_tls_context_must_be_an_array(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new PulsarFactory())->create($this->config([
            'service_url' => 'pulsar://127.0.0.1:6650',
            'tls_context' => 'none',
        ]));
    }

    public function test_topic_must_be_a_non_empty_string(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new PulsarFactory())->create($this->config([
            'service_url' => 'pulsar://127.0.0.1:6650',
            'topic' => '',
        ]));
    }

    public function test_subscription_must_be_a_non_empty_string(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new PulsarFactory())->create($this->config([
            'service_url' => 'pulsar://127.0.0.1:6650',
            'subscription' => '',
        ]));
    }

    public function test_consumer_name_must_be_a_non_empty_string(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new PulsarFactory())->create($this->config([
            'service_url' => 'pulsar://127.0.0.1:6650',
            'consumer_name' => '',
        ]));
    }

    public function test_timeout_must_be_positive(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new PulsarFactory())->create($this->config([
            'service_url' => 'pulsar://127.0.0.1:6650',
            'timeout' => 0,
        ]));
    }

    public function test_timeout_must_be_numeric(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new PulsarFactory())->create($this->config([
            'service_url' => 'pulsar://127.0.0.1:6650',
            'timeout' => '10',
        ]));
    }

    /**
     * @param array<string, mixed> $options
     */
    private function config(array $options): ConnectionConfig
    {
        return ConnectionConfig::fromArray('pulsar', ['driver' => 'pulsar'] + $options);
    }
}
