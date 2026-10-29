<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Kafka;

use Erikwang2013\Messaging\Driver\Kafka\ConsumerProtocol;
use PHPUnit\Framework\TestCase;

final class ConsumerProtocolTest extends TestCase
{
    public function test_subscription_roundtrip(): void
    {
        $metadata = ConsumerProtocol::encodeSubscription(['orders', 'events']);

        self::assertSame(['orders', 'events'], ConsumerProtocol::decodeSubscription($metadata));
    }

    public function test_subscription_decode_tolerates_optional_v1_fields(): void
    {
        $metadata = ConsumerProtocol::encodeSubscription(['orders'])
            . hex2bin('00000000')
            . hex2bin('00000000');

        self::assertSame(['orders'], ConsumerProtocol::decodeSubscription($metadata));
    }

    public function test_assignment_roundtrip_sorts_partitions(): void
    {
        $encoded = ConsumerProtocol::encodeAssignment(['orders' => [3, 1, 2], 'events' => [0]]);

        self::assertSame(
            ['orders' => [1, 2, 3], 'events' => [0]],
            ConsumerProtocol::decodeAssignment($encoded),
        );
    }

    public function test_range_assignor_splits_each_topic_evenly(): void
    {
        $assignments = ConsumerProtocol::rangeAssign(
            [
                'member-b' => ['orders'],
                'member-a' => ['orders', 'events'],
                'member-c' => ['events'],
            ],
            ['orders' => 5, 'events' => 3],
        );

        self::assertSame([
            'member-a' => ['events' => [0, 1], 'orders' => [0, 1, 2]],
            'member-b' => ['orders' => [3, 4]],
            'member-c' => ['events' => [2]],
        ], $assignments);
    }

    public function test_range_assignor_handles_more_members_than_partitions(): void
    {
        $assignments = ConsumerProtocol::rangeAssign(
            [
                'a' => ['t'],
                'b' => ['t'],
                'c' => ['t'],
            ],
            ['t' => 2],
        );

        self::assertSame(['t' => [0]], $assignments['a']);
        self::assertSame(['t' => [1]], $assignments['b']);
        self::assertSame([], $assignments['c']);
    }
}
