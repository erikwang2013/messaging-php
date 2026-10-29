<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\RocketMq;

use Erikwang2013\Messaging\Driver\RocketMq\RocketMqRoute;
use Erikwang2013\Messaging\Exception\ProtocolException;
use PHPUnit\Framework\TestCase;

final class RocketMqRouteTest extends TestCase
{
    public function test_topic_route_data_is_normalized(): void
    {
        $route = RocketMqRoute::parse((string) json_encode([
            'orderTopicConf' => '',
            'queueDatas' => [
                ['brokerName' => 'broker-a', 'readQueueNums' => 4, 'writeQueueNums' => 4, 'perm' => 6, 'topicSynFlag' => 0],
                ['brokerName' => 'broker-b', 'readQueueNums' => 2, 'writeQueueNums' => 0, 'perm' => 4, 'topicSynFlag' => 0],
            ],
            'brokerDatas' => [
                [
                    'cluster' => 'DefaultCluster',
                    'brokerName' => 'broker-a',
                    'brokerAddrs' => [0 => '10.0.0.1:10911', 1 => '10.0.0.2:10911'],
                ],
                [
                    'cluster' => 'DefaultCluster',
                    'brokerName' => 'broker-b',
                    'brokerAddrs' => ['0' => '10.0.0.3:10911'],
                ],
            ],
            'filterServerTable' => [],
        ]));

        self::assertSame([
            'broker-a' => '10.0.0.1:10911',
            'broker-b' => '10.0.0.3:10911',
        ], $route['brokers']);

        self::assertSame([
            ['broker' => 'broker-a', 'readQueueNums' => 4, 'writeQueueNums' => 4, 'perm' => 6],
            ['broker' => 'broker-b', 'readQueueNums' => 2, 'writeQueueNums' => 0, 'perm' => 4],
        ], $route['queues']);
    }

    public function test_broker_without_a_master_address_is_skipped(): void
    {
        $route = RocketMqRoute::parse((string) json_encode([
            'queueDatas' => [],
            'brokerDatas' => [
                ['brokerName' => 'slave-only', 'brokerAddrs' => [1 => '10.0.0.2:10911']],
            ],
        ]));

        self::assertSame([], $route['brokers']);
    }

    public function test_invalid_json_is_rejected(): void
    {
        $this->expectException(ProtocolException::class);

        RocketMqRoute::parse('{');
    }

    public function test_broker_entry_requires_addresses(): void
    {
        $this->expectException(ProtocolException::class);

        RocketMqRoute::parse('{"brokerDatas":[{"brokerName":"a"}],"queueDatas":[]}');
    }

    public function test_queue_entry_requires_integer_counts(): void
    {
        $this->expectException(ProtocolException::class);

        RocketMqRoute::parse(
            '{"brokerDatas":[],"queueDatas":[{"brokerName":"a","readQueueNums":"4","writeQueueNums":4,"perm":6}]}',
        );
    }
}
