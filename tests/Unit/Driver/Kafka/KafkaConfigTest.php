<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Kafka;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Driver\Kafka\KafkaConfig;
use PHPUnit\Framework\TestCase;

final class KafkaConfigTest extends TestCase
{
    public function test_defaults(): void
    {
        $config = KafkaConfig::fromConnection(self::connection('kafka', [
            'brokers' => ['broker:9092'],
        ]));

        self::assertSame([['host' => 'broker', 'port' => 9092]], $config->brokers);
        self::assertSame('messaging', $config->groupId);
        self::assertSame('messaging-php', $config->clientId);
        self::assertSame(10.0, $config->timeout);
        self::assertFalse($config->tls);
        self::assertSame('latest', $config->autoOffsetReset);
        self::assertSame(1, $config->acks);
        self::assertSame(500, $config->fetchMaxWaitMs);
        self::assertSame(10000, $config->sessionTimeoutMs);
        self::assertSame(1, $config->maxPollRecords);
        self::assertSame('kafka', $config->alias);
    }

    public function test_alias_default_ports_and_address_forms(): void
    {
        $eventHubs = KafkaConfig::fromConnection(self::connection('eventhubs', [
            'brokers' => ['namespace.servicebus.windows.net', '[::1]:9095', 'host:1234', 'host2'],
        ]), 'eventhubs');

        self::assertSame([
            ['host' => 'namespace.servicebus.windows.net', 'port' => 9093],
            ['host' => '::1', 'port' => 9095],
            ['host' => 'host', 'port' => 1234],
            ['host' => 'host2', 'port' => 9093],
        ], $eventHubs->brokers);

        $redpanda = KafkaConfig::fromConnection(self::connection('redpanda', [
            'brokers' => 'a,b:19092',
        ]), 'redpanda');

        self::assertSame([
            ['host' => 'a', 'port' => 9092],
            ['host' => 'b', 'port' => 19092],
        ], $redpanda->brokers);
    }

    public function test_options_are_parsed(): void
    {
        $config = KafkaConfig::fromConnection(self::connection('kafka', [
            'brokers' => ['broker'],
            'group_id' => 'workers',
            'client_id' => 'app',
            'timeout' => 3,
            'tls' => true,
            'tls_context' => ['verify_peer' => false],
            'auto_offset_reset' => 'earliest',
            'acks' => -1,
            'fetch_max_wait_ms' => 0,
            'session_timeout_ms' => 30000,
            'max_poll_records' => 50,
        ]));

        self::assertSame('workers', $config->groupId);
        self::assertSame('app', $config->clientId);
        self::assertSame(3.0, $config->timeout);
        self::assertTrue($config->tls);
        self::assertSame(['verify_peer' => false], $config->tlsContext);
        self::assertSame('earliest', $config->autoOffsetReset);
        self::assertSame(-1, $config->acks);
        self::assertSame(0, $config->fetchMaxWaitMs);
        self::assertSame(30000, $config->sessionTimeoutMs);
        self::assertSame(50, $config->maxPollRecords);
    }

    public function test_invalid_options_are_rejected(): void
    {
        $invalid = [
            [['brokers' => []], 'brokers'],
            [['brokers' => ['']], 'brokers'],
            [['brokers' => ['host:0']], 'Invalid Kafka broker address'],
            [['brokers' => ['[::1']], 'Invalid Kafka broker address'],
            [['brokers' => ['host'], 'group_id' => ''], 'group_id'],
            [['brokers' => ['host'], 'client_id' => 1], 'client_id'],
            [['brokers' => ['host'], 'timeout' => 0], 'timeout'],
            [['brokers' => ['host'], 'tls' => 'yes'], 'tls'],
            [['brokers' => ['host'], 'tls_context' => 'yes'], 'tls_context'],
            [['brokers' => ['host'], 'auto_offset_reset' => 'middle'], 'auto_offset_reset'],
            [['brokers' => ['host'], 'acks' => 0], 'acks'],
            [['brokers' => ['host'], 'fetch_max_wait_ms' => -1], 'fetch_max_wait_ms'],
            [['brokers' => ['host'], 'session_timeout_ms' => 0], 'session_timeout_ms'],
            [['brokers' => ['host'], 'max_poll_records' => 0], 'max_poll_records'],
        ];

        foreach ($invalid as [$options, $message]) {
            try {
                KafkaConfig::fromConnection(self::connection('kafka', $options));
                self::fail('Expected validation failure for ' . $message);
            } catch (\InvalidArgumentException $exception) {
                self::assertStringContainsString($message, $exception->getMessage());
            }
        }
    }

    public function test_unknown_alias_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        KafkaConfig::fromConnection(self::connection('kafka', ['brokers' => ['host']]), 'pulsar');
    }

    /**
     * @param array<string, mixed> $options
     */
    private static function connection(string $driver, array $options): ConnectionConfig
    {
        return ConnectionConfig::fromArray('test', ['driver' => $driver] + $options);
    }
}
