<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\RocketMq;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Driver\RocketMq\RocketMqConfig;
use Erikwang2013\Messaging\Driver\RocketMq\RocketMqDriver;
use Erikwang2013\Messaging\Driver\RocketMq\RocketMqFactory;
use PHPUnit\Framework\TestCase;

final class RocketMqConfigTest extends TestCase
{
    public function test_defaults_are_applied(): void
    {
        $config = RocketMqConfig::fromConnection($this->config(['nameserver' => '127.0.0.1:9876']));

        self::assertSame('127.0.0.1', $config->nameserverHost());
        self::assertSame(9876, $config->nameserverPort());
        self::assertSame('messaging', $config->group());
        self::assertSame(10.0, $config->timeout());
        self::assertSame(30.0, $config->heartbeatInterval());
        self::assertFalse($config->consumeFromFirst());
        self::assertSame(1, $config->pullBatchSize());
    }

    public function test_factory_builds_a_driver(): void
    {
        $driver = (new RocketMqFactory())->create($this->config(['nameserver' => 'localhost']));

        self::assertInstanceOf(RocketMqDriver::class, $driver);
    }

    public function test_nameserver_is_required(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        RocketMqConfig::fromConnection($this->config([]));
    }

    public function test_optional_topic_is_used_for_empty_destinations(): void
    {
        $config = RocketMqConfig::fromConnection($this->config([
            'nameserver' => 'localhost',
            'topic' => 'default-topic',
        ]));

        self::assertSame('default-topic', $config->destination(''));
    }

    public function test_empty_destination_without_default_topic_is_rejected(): void
    {
        $config = RocketMqConfig::fromConnection($this->config(['nameserver' => 'localhost']));

        $this->expectException(\InvalidArgumentException::class);

        $config->destination('');
    }

    public function test_invalid_destination_names_are_rejected(): void
    {
        $config = RocketMqConfig::fromConnection($this->config(['nameserver' => 'localhost']));

        $this->expectException(\InvalidArgumentException::class);

        $config->destination('bad topic');
    }

    public function test_consume_from_only_accepts_first_or_last(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        RocketMqConfig::fromConnection($this->config([
            'nameserver' => 'localhost',
            'consume_from' => 'middle',
        ]));
    }

    public function test_consume_from_first_is_parsed(): void
    {
        $config = RocketMqConfig::fromConnection($this->config([
            'nameserver' => 'localhost',
            'consume_from' => 'first',
        ]));

        self::assertTrue($config->consumeFromFirst());
    }

    public function test_pull_batch_size_must_be_positive(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        RocketMqConfig::fromConnection($this->config([
            'nameserver' => 'localhost',
            'pull_batch_size' => 0,
        ]));
    }

    public function test_heartbeat_interval_is_parsed(): void
    {
        $config = RocketMqConfig::fromConnection($this->config([
            'nameserver' => 'localhost',
            'heartbeat_interval' => 5,
        ]));

        self::assertSame(5.0, $config->heartbeatInterval());
    }

    public function test_heartbeat_interval_must_be_at_least_one_second(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        RocketMqConfig::fromConnection($this->config([
            'nameserver' => 'localhost',
            'heartbeat_interval' => 0.5,
        ]));
    }

    public function test_timeout_must_be_positive(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        RocketMqConfig::fromConnection($this->config([
            'nameserver' => 'localhost',
            'timeout' => 0,
        ]));
    }

    public function test_plain_host_uses_the_default_port(): void
    {
        self::assertSame(['ns.example.com', 9876], RocketMqConfig::parseAddress('ns.example.com', 9876));
        self::assertSame(['ns.example.com', 19876], RocketMqConfig::parseAddress('ns.example.com:19876', 9876));
    }

    public function test_bracketed_ipv6_addresses_are_parsed(): void
    {
        self::assertSame(['::1', 9876], RocketMqConfig::parseAddress('[::1]', 9876));
        self::assertSame(['::1', 19876], RocketMqConfig::parseAddress('[::1]:19876', 9876));
    }

    public function test_unbracketed_ipv6_addresses_are_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        RocketMqConfig::parseAddress('::1:9876', 9876);
    }

    public function test_invalid_ports_are_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        RocketMqConfig::parseAddress('localhost:99999', 9876);
    }

    /**
     * @param array<string, mixed> $options
     */
    private function config(array $options): ConnectionConfig
    {
        return ConnectionConfig::fromArray('rocketmq', ['driver' => 'rocketmq'] + $options);
    }
}
