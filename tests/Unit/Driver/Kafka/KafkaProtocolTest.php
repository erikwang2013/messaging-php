<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Kafka;

use Erikwang2013\Messaging\Driver\Kafka\Decoder;
use Erikwang2013\Messaging\Driver\Kafka\Encoder;
use Erikwang2013\Messaging\Driver\Kafka\KafkaProtocol;
use PHPUnit\Framework\TestCase;

final class KafkaProtocolTest extends TestCase
{
    public function test_api_versions_request_is_flexible(): void
    {
        self::assertSame(
            '0e6d6573736167696e672d70687006312e302e3000',
            bin2hex(KafkaProtocol::apiVersionsRequest()),
        );
    }

    public function test_metadata_request_lists_topics_and_allows_auto_creation(): void
    {
        $decoder = new Decoder(KafkaProtocol::metadataRequest(['orders', 'events']));

        self::assertSame(2, $decoder->int32());
        self::assertSame('orders', $decoder->string());
        self::assertSame('events', $decoder->string());
        self::assertTrue($decoder->bool());
    }

    public function test_produce_request_encoding(): void
    {
        $bytes = KafkaProtocol::produceRequest('orders', [0 => 'BATCH', 2 => 'BATCH2'], -1, 5000);
        $decoder = new Decoder($bytes);

        self::assertNull($decoder->nullableString());
        self::assertSame(-1, $decoder->int16());
        self::assertSame(5000, $decoder->int32());
        self::assertSame(1, $decoder->int32());
        self::assertSame('orders', $decoder->string());
        self::assertSame(2, $decoder->int32());
        self::assertSame(0, $decoder->int32());
        self::assertSame('BATCH', $decoder->bytes());
        self::assertSame(2, $decoder->int32());
        self::assertSame('BATCH2', $decoder->bytes());
        self::assertSame(0, $decoder->remaining());
    }

    public function test_fetch_request_encoding(): void
    {
        $bytes = KafkaProtocol::fetchRequest(['orders' => [0 => 5, 2 => 9]], 500, 1024, 2048);
        $decoder = new Decoder($bytes);

        self::assertSame(-1, $decoder->int32());
        self::assertSame(500, $decoder->int32());
        self::assertSame(1, $decoder->int32());
        self::assertSame(1024, $decoder->int32());
        self::assertSame(0, $decoder->int8());
        self::assertSame(1, $decoder->int32());
        self::assertSame('orders', $decoder->string());
        self::assertSame(2, $decoder->int32());
        self::assertSame(0, $decoder->int32());
        self::assertSame(5, $decoder->int64());
        self::assertSame(2048, $decoder->int32());
        self::assertSame(2, $decoder->int32());
        self::assertSame(9, $decoder->int64());
        self::assertSame(2048, $decoder->int32());
        self::assertSame(0, $decoder->remaining());
    }

    public function test_list_offsets_request_encoding(): void
    {
        $decoder = new Decoder(KafkaProtocol::listOffsetsRequest('orders', [1], -2));

        self::assertSame(-1, $decoder->int32());
        self::assertSame(1, $decoder->int32());
        self::assertSame('orders', $decoder->string());
        self::assertSame(1, $decoder->int32());
        self::assertSame(1, $decoder->int32());
        self::assertSame(-2, $decoder->int64());
    }

    public function test_group_request_encodings(): void
    {
        $decoder = new Decoder(KafkaProtocol::findCoordinatorRequest('grp'));
        self::assertSame('grp', $decoder->string());
        self::assertSame(0, $decoder->int8());

        $decoder = new Decoder(KafkaProtocol::joinGroupRequest('grp', 'member', 'range', 'META', 10000, 12000));
        self::assertSame('grp', $decoder->string());
        self::assertSame(10000, $decoder->int32());
        self::assertSame(12000, $decoder->int32());
        self::assertSame('member', $decoder->string());
        self::assertSame('consumer', $decoder->string());
        self::assertSame(1, $decoder->int32());
        self::assertSame('range', $decoder->string());
        self::assertSame('META', $decoder->bytes());

        $decoder = new Decoder(KafkaProtocol::syncGroupRequest('grp', 3, 'm1', ['m1' => 'A', 'm2' => 'B']));
        self::assertSame('grp', $decoder->string());
        self::assertSame(3, $decoder->int32());
        self::assertSame('m1', $decoder->string());
        self::assertSame(2, $decoder->int32());
        self::assertSame('m1', $decoder->string());
        self::assertSame('A', $decoder->bytes());
        self::assertSame('m2', $decoder->string());
        self::assertSame('B', $decoder->bytes());

        $decoder = new Decoder(KafkaProtocol::heartbeatRequest('grp', 4, 'member'));
        self::assertSame('grp', $decoder->string());
        self::assertSame(4, $decoder->int32());
        self::assertSame('member', $decoder->string());

        $decoder = new Decoder(KafkaProtocol::leaveGroupRequest('grp', 'member'));
        self::assertSame('grp', $decoder->string());
        self::assertSame('member', $decoder->string());
    }

    public function test_offset_commit_request_encoding(): void
    {
        $decoder = new Decoder(KafkaProtocol::offsetCommitRequest('grp', 7, 'member', ['orders' => [0 => 11]]));

        self::assertSame('grp', $decoder->string());
        self::assertSame(7, $decoder->int32());
        self::assertSame('member', $decoder->string());
        self::assertSame(-1, $decoder->int64());
        self::assertSame(1, $decoder->int32());
        self::assertSame('orders', $decoder->string());
        self::assertSame(1, $decoder->int32());
        self::assertSame(0, $decoder->int32());
        self::assertSame(11, $decoder->int64());
        self::assertNull($decoder->nullableString());
    }

    public function test_offset_fetch_request_encoding(): void
    {
        $decoder = new Decoder(KafkaProtocol::offsetFetchRequest('grp', ['orders' => [0, 1]]));

        self::assertSame('grp', $decoder->string());
        self::assertSame(1, $decoder->int32());
        self::assertSame('orders', $decoder->string());
        self::assertSame(2, $decoder->int32());
        self::assertSame(0, $decoder->int32());
        self::assertSame(1, $decoder->int32());
        self::assertSame(0, $decoder->remaining());
    }

    public function test_parse_api_versions_v0_and_v3(): void
    {
        $v0 = (new Encoder())
            ->int16(0)
            ->int32(2)
            ->int16(0)->int16(0)->int16(8)
            ->int16(1)->int16(0)->int16(12)
            ->toString();

        self::assertSame(
            ['error' => 0, 'versions' => [0 => [0, 8], 1 => [0, 12]]],
            KafkaProtocol::parseApiVersionsResponse(new Decoder($v0), 0),
        );

        $v3 = (new Encoder())
            ->int16(0)
            ->uvarint(4)
            ->int16(0)->int16(0)->int16(8)->uvarint(0)
            ->int16(3)->int16(0)->int16(4)->uvarint(0)
            ->int16(18)->int16(0)->int16(3)->uvarint(0)
            ->int32(0)
            ->uvarint(0)
            ->toString();

        self::assertSame(
            ['error' => 0, 'versions' => [0 => [0, 8], 3 => [0, 4], 18 => [0, 3]]],
            KafkaProtocol::parseApiVersionsResponse(new Decoder($v3), 3),
        );
    }

    public function test_parse_metadata_response(): void
    {
        $bytes = (new Encoder())
            ->int32(0)
            ->int32(1)
            ->int32(7)->string('127.0.0.1')->int32(9092)->nullableString(null)
            ->nullableString('cluster')
            ->int32(7)
            ->int32(1)
            ->int16(0)->string('orders')->bool(false)
            ->int32(1)
            ->int16(0)->int32(0)->int32(7)->int32(1)->int32(7)->int32(1)->int32(7);

        self::assertSame([
            'brokers' => [7 => ['host' => '127.0.0.1', 'port' => 9092]],
            'topics' => [
                'orders' => [
                    'error' => 0,
                    'partitions' => [0 => ['leader' => 7, 'error' => 0]],
                ],
            ],
        ], KafkaProtocol::parseMetadataResponse(new Decoder($bytes->toString())));
    }

    public function test_parse_produce_response_reads_throttle_after_partitions(): void
    {
        $bytes = (new Encoder())
            ->int32(1)
            ->string('orders')
            ->int32(1)
            ->int32(0)
            ->int16(0)
            ->int64(42)
            ->int64(-1)
            ->int32(5)
            ->toString();

        self::assertSame(
            ['orders' => [0 => ['error' => 0, 'base_offset' => 42]]],
            KafkaProtocol::parseProduceResponse(new Decoder($bytes)),
        );
    }

    public function test_parse_fetch_response_with_aborted_transactions(): void
    {
        $bytes = (new Encoder())
            ->int32(0)
            ->int32(1)
            ->string('orders')
            ->int32(1)
            ->int32(0)
            ->int16(0)
            ->int64(100)
            ->int64(100)
            ->int32(1)
            ->int64(11)
            ->int64(3)
            ->bytes('RECORDS')
            ->toString();

        self::assertSame([
            'orders' => [
                0 => ['error' => 0, 'high_watermark' => 100, 'records' => 'RECORDS'],
            ],
        ], KafkaProtocol::parseFetchResponse(new Decoder($bytes)));
    }

    public function test_parse_list_offsets_response(): void
    {
        $bytes = (new Encoder())
            ->int32(1)
            ->string('orders')
            ->int32(1)
            ->int32(0)
            ->int16(0)
            ->int64(-1)
            ->int64(17)
            ->toString();

        self::assertSame(
            ['orders' => [0 => ['error' => 0, 'offset' => 17]]],
            KafkaProtocol::parseListOffsetsResponse(new Decoder($bytes)),
        );
    }

    public function test_parse_find_coordinator_response(): void
    {
        $bytes = (new Encoder())
            ->int32(0)
            ->int16(0)
            ->nullableString(null)
            ->int32(7)
            ->string('coordinator')
            ->int32(9092)
            ->toString();

        self::assertSame([
            'error' => 0,
            'message' => null,
            'node_id' => 7,
            'host' => 'coordinator',
            'port' => 9092,
        ], KafkaProtocol::parseFindCoordinatorResponse(new Decoder($bytes)));
    }

    public function test_parse_join_and_sync_group_responses(): void
    {
        $bytes = (new Encoder())
            ->int32(0)
            ->int16(0)
            ->int32(3)
            ->string('range')
            ->string('m1')
            ->string('m2')
            ->int32(2)
            ->string('m1')->bytes('A')
            ->string('m2')->bytes('B')
            ->toString();

        self::assertSame([
            'error' => 0,
            'generation' => 3,
            'protocol' => 'range',
            'leader' => 'm1',
            'member_id' => 'm2',
            'members' => ['m1' => 'A', 'm2' => 'B'],
        ], KafkaProtocol::parseJoinGroupResponse(new Decoder($bytes)));

        $sync = (new Encoder())->int32(0)->int16(0)->bytes('ASSIGN')->toString();
        self::assertSame(
            ['error' => 0, 'assignment' => 'ASSIGN'],
            KafkaProtocol::parseSyncGroupResponse(new Decoder($sync)),
        );
    }

    public function test_parse_heartbeat_leave_group_and_commit_responses(): void
    {
        self::assertSame(27, KafkaProtocol::parseHeartbeatResponse(new Decoder((new Encoder())->int32(0)->int16(27)->toString())));
        self::assertSame(0, KafkaProtocol::parseLeaveGroupResponse(new Decoder((new Encoder())->int32(0)->int16(0)->toString())));

        $commit = (new Encoder())
            ->int32(1)
            ->string('orders')
            ->int32(2)
            ->int32(0)->int16(0)
            ->int32(1)->int16(7)
            ->toString();

        self::assertSame(
            ['orders' => [0 => 0, 1 => 7]],
            KafkaProtocol::parseOffsetCommitResponse(new Decoder($commit)),
        );
    }

    public function test_parse_offset_fetch_response_reads_errors_per_partition_and_at_the_end(): void
    {
        $bytes = (new Encoder())
            ->int32(1)
            ->string('orders')
            ->int32(1)
            ->int32(0)
            ->int64(12)
            ->nullableString(null)
            ->int16(0)
            ->int16(0)
            ->toString();

        self::assertSame([
            'error' => 0,
            'topics' => ['orders' => [0 => ['offset' => 12, 'error' => 0]]],
        ], KafkaProtocol::parseOffsetFetchResponse(new Decoder($bytes)));
    }
}
