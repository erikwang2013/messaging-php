<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

/**
 * Wire encoders/parsers for the stable Kafka API subset used by the driver.
 * All versions are non-flexible except ApiVersions v3.
 */
final class KafkaProtocol
{
    public const API_PRODUCE = 0;
    public const API_FETCH = 1;
    public const API_LIST_OFFSETS = 2;
    public const API_METADATA = 3;
    public const API_OFFSET_COMMIT = 8;
    public const API_OFFSET_FETCH = 9;
    public const API_FIND_COORDINATOR = 10;
    public const API_JOIN_GROUP = 11;
    public const API_HEARTBEAT = 12;
    public const API_LEAVE_GROUP = 13;
    public const API_SYNC_GROUP = 14;
    public const API_API_VERSIONS = 18;

    public const VERSION_PRODUCE = 3;
    public const VERSION_FETCH = 4;
    public const VERSION_LIST_OFFSETS = 1;
    public const VERSION_METADATA = 4;
    public const VERSION_OFFSET_COMMIT = 2;
    public const VERSION_OFFSET_FETCH = 2;
    public const VERSION_FIND_COORDINATOR = 1;
    public const VERSION_JOIN_GROUP = 2;
    public const VERSION_HEARTBEAT = 1;
    public const VERSION_LEAVE_GROUP = 2;
    public const VERSION_SYNC_GROUP = 2;
    public const VERSION_API_VERSIONS = 3;

    public static function apiVersionsRequest(): string
    {
        return (new Encoder())
            ->compactString('messaging-php')
            ->compactString('1.0.0')
            ->uvarint(0)
            ->toString();
    }

    public static function metadataRequest(array $topics): string
    {
        $encoder = (new Encoder())->int32(count($topics));
        foreach ($topics as $topic) {
            $encoder->string($topic);
        }

        return $encoder->bool(true)->toString();
    }

    /**
     * @param array<int, string> $partitionBatches partition index => encoded record batch
     */
    public static function produceRequest(string $topic, array $partitionBatches, int $acks, int $timeoutMs): string
    {
        $encoder = (new Encoder())
            ->nullableString(null)
            ->int16($acks)
            ->int32($timeoutMs)
            ->int32(1)
            ->string($topic)
            ->int32(count($partitionBatches));

        foreach ($partitionBatches as $partition => $batch) {
            $encoder->int32($partition)->bytes($batch);
        }

        return $encoder->toString();
    }

    /**
     * @param array<string, array<int, int>> $offsets topic => partition => fetch offset
     */
    public static function fetchRequest(
        array $offsets,
        int $maxWaitMs,
        int $maxBytes,
        int $partitionMaxBytes,
    ): string {
        $encoder = (new Encoder())
            ->int32(-1)
            ->int32($maxWaitMs)
            ->int32(1)
            ->int32($maxBytes)
            ->int8(0)
            ->int32(count($offsets));

        foreach ($offsets as $topic => $partitions) {
            $encoder->string((string) $topic)->int32(count($partitions));
            foreach ($partitions as $partition => $offset) {
                $encoder->int32($partition)->int64($offset)->int32($partitionMaxBytes);
            }
        }

        return $encoder->toString();
    }

    /**
     * @param list<int> $partitions
     */
    public static function listOffsetsRequest(string $topic, array $partitions, int $timestamp): string
    {
        $encoder = (new Encoder())
            ->int32(-1)
            ->int32(1)
            ->string($topic)
            ->int32(count($partitions));

        foreach ($partitions as $partition) {
            $encoder->int32($partition)->int64($timestamp);
        }

        return $encoder->toString();
    }

    public static function findCoordinatorRequest(string $key): string
    {
        return (new Encoder())->string($key)->int8(0)->toString();
    }

    public static function joinGroupRequest(
        string $groupId,
        string $memberId,
        string $protocolName,
        string $metadata,
        int $sessionTimeoutMs,
        int $rebalanceTimeoutMs,
    ): string {
        return (new Encoder())
            ->string($groupId)
            ->int32($sessionTimeoutMs)
            ->int32($rebalanceTimeoutMs)
            ->string($memberId)
            ->string('consumer')
            ->int32(1)
            ->string($protocolName)
            ->bytes($metadata)
            ->toString();
    }

    /**
     * @param array<string, string> $assignments member id => assignment bytes
     */
    public static function syncGroupRequest(
        string $groupId,
        int $generationId,
        string $memberId,
        array $assignments,
    ): string {
        $encoder = (new Encoder())
            ->string($groupId)
            ->int32($generationId)
            ->string($memberId)
            ->int32(count($assignments));

        foreach ($assignments as $member => $assignment) {
            $encoder->string((string) $member)->bytes($assignment);
        }

        return $encoder->toString();
    }

    public static function heartbeatRequest(string $groupId, int $generationId, string $memberId): string
    {
        return (new Encoder())->string($groupId)->int32($generationId)->string($memberId)->toString();
    }

    public static function leaveGroupRequest(string $groupId, string $memberId): string
    {
        return (new Encoder())->string($groupId)->string($memberId)->toString();
    }

    /**
     * @param array<string, array<int, int>> $offsets topic => partition => next offset
     */
    public static function offsetCommitRequest(
        string $groupId,
        int $generationId,
        string $memberId,
        array $offsets,
        int $retentionMs = -1,
    ): string {
        $encoder = (new Encoder())
            ->string($groupId)
            ->int32($generationId)
            ->string($memberId)
            ->int64($retentionMs)
            ->int32(count($offsets));

        foreach ($offsets as $topic => $partitions) {
            $encoder->string((string) $topic)->int32(count($partitions));
            foreach ($partitions as $partition => $offset) {
                $encoder
                    ->int32($partition)
                    ->int64($offset)
                    ->nullableString(null);
            }
        }

        return $encoder->toString();
    }

    /**
     * @param array<string, list<int>> $topics topic => partition indexes
     */
    public static function offsetFetchRequest(string $groupId, array $topics): string
    {
        $encoder = (new Encoder())->string($groupId)->int32(count($topics));

        foreach ($topics as $topic => $partitions) {
            $encoder->string((string) $topic)->int32(count($partitions));
            foreach ($partitions as $partition) {
                $encoder->int32($partition);
            }
        }

        return $encoder->toString();
    }

    /**
     * @return array{error: int, versions: array<int, array{0: int, 1: int}>}
     */
    public static function parseApiVersionsResponse(Decoder $decoder, int $requestVersion): array
    {
        $error = $decoder->int16();
        $versions = [];

        if ($requestVersion >= 3) {
            $count = $decoder->uvarint();
            for ($i = 1; $i < $count; $i++) {
                $apiKey = $decoder->int16();
                $min = $decoder->int16();
                $max = $decoder->int16();
                $decoder->skipTags();
                $versions[$apiKey] = [$min, $max];
            }

            $decoder->int32(); // throttle time
            $decoder->skipTags();
        } else {
            $count = $decoder->arrayLength() ?? 0;
            for ($i = 0; $i < $count; $i++) {
                $apiKey = $decoder->int16();
                $min = $decoder->int16();
                $max = $decoder->int16();
                $versions[$apiKey] = [$min, $max];
            }
        }

        return ['error' => $error, 'versions' => $versions];
    }

    /**
     * @return array{brokers: array<int, array{host: string, port: int}>, topics: array<string, array{error: int, partitions: array<int, array{leader: int, error: int}>}>}
     */
    public static function parseMetadataResponse(Decoder $decoder): array
    {
        $decoder->int32(); // throttle time

        $brokers = [];
        $brokerCount = $decoder->arrayLength() ?? 0;
        for ($i = 0; $i < $brokerCount; $i++) {
            $nodeId = $decoder->int32();
            $host = $decoder->string();
            $port = $decoder->int32();
            $decoder->nullableString(); // rack
            $brokers[$nodeId] = ['host' => $host, 'port' => $port];
        }

        $decoder->nullableString(); // cluster id
        $decoder->int32(); // controller id

        $topics = [];
        $topicCount = $decoder->arrayLength() ?? 0;
        for ($i = 0; $i < $topicCount; $i++) {
            $error = $decoder->int16();
            $name = $decoder->string();
            $decoder->bool(); // is internal

            $partitions = [];
            $partitionCount = $decoder->arrayLength() ?? 0;
            for ($p = 0; $p < $partitionCount; $p++) {
                $partitionError = $decoder->int16();
                $index = $decoder->int32();
                $leader = $decoder->int32();
                $decoder->skip($decoder->int32() * 4); // replicas
                $decoder->skip($decoder->int32() * 4); // in-sync replicas
                $partitions[$index] = ['leader' => $leader, 'error' => $partitionError];
            }

            $topics[$name] = ['error' => $error, 'partitions' => $partitions];
        }

        return ['brokers' => $brokers, 'topics' => $topics];
    }

    /**
     * @return array<string, array<int, array{error: int, base_offset: int}>>
     */
    public static function parseProduceResponse(Decoder $decoder): array
    {
        $topics = [];
        $topicCount = $decoder->arrayLength() ?? 0;
        for ($i = 0; $i < $topicCount; $i++) {
            $name = $decoder->string();
            $partitionCount = $decoder->arrayLength() ?? 0;
            for ($p = 0; $p < $partitionCount; $p++) {
                $index = $decoder->int32();
                $error = $decoder->int16();
                $baseOffset = $decoder->int64();
                $decoder->int64(); // log append time
                $topics[$name][$index] = ['error' => $error, 'base_offset' => $baseOffset];
            }
        }

        $decoder->int32(); // throttle time

        return $topics;
    }

    /**
     * @return array<string, array<int, array{error: int, high_watermark: int, records: ?string}>>
     */
    public static function parseFetchResponse(Decoder $decoder): array
    {
        $decoder->int32(); // throttle time

        $topics = [];
        $topicCount = $decoder->arrayLength() ?? 0;
        for ($i = 0; $i < $topicCount; $i++) {
            $name = $decoder->string();
            $partitionCount = $decoder->arrayLength() ?? 0;
            for ($p = 0; $p < $partitionCount; $p++) {
                $index = $decoder->int32();
                $error = $decoder->int16();
                $highWatermark = $decoder->int64();
                $decoder->int64(); // last stable offset
                $abortedCount = $decoder->arrayLength() ?? 0;
                $decoder->skip($abortedCount * 16); // producer id + first offset
                $records = $decoder->bytes();
                $topics[$name][$index] = [
                    'error' => $error,
                    'high_watermark' => $highWatermark,
                    'records' => $records,
                ];
            }
        }

        return $topics;
    }

    /**
     * @return array<string, array<int, array{error: int, offset: int}>>
     */
    public static function parseListOffsetsResponse(Decoder $decoder): array
    {
        $topics = [];
        $topicCount = $decoder->arrayLength() ?? 0;
        for ($i = 0; $i < $topicCount; $i++) {
            $name = $decoder->string();
            $partitionCount = $decoder->arrayLength() ?? 0;
            for ($p = 0; $p < $partitionCount; $p++) {
                $index = $decoder->int32();
                $error = $decoder->int16();
                $decoder->int64(); // timestamp
                $offset = $decoder->int64();
                $topics[$name][$index] = ['error' => $error, 'offset' => $offset];
            }
        }

        return $topics;
    }

    /**
     * @return array{error: int, message: ?string, node_id: int, host: string, port: int}
     */
    public static function parseFindCoordinatorResponse(Decoder $decoder): array
    {
        $decoder->int32(); // throttle time

        return [
            'error' => $decoder->int16(),
            'message' => $decoder->nullableString(),
            'node_id' => $decoder->int32(),
            'host' => $decoder->string(),
            'port' => $decoder->int32(),
        ];
    }

    /**
     * @return array{error: int, generation: int, protocol: string, leader: string, member_id: string, members: array<string, string>}
     */
    public static function parseJoinGroupResponse(Decoder $decoder): array
    {
        $decoder->int32(); // throttle time

        $error = $decoder->int16();
        $generation = $decoder->int32();
        $protocol = $decoder->string();
        $leader = $decoder->string();
        $memberId = $decoder->string();

        $members = [];
        $memberCount = $decoder->arrayLength() ?? 0;
        for ($i = 0; $i < $memberCount; $i++) {
            $id = $decoder->string();
            $members[$id] = $decoder->bytes() ?? '';
        }

        return [
            'error' => $error,
            'generation' => $generation,
            'protocol' => $protocol,
            'leader' => $leader,
            'member_id' => $memberId,
            'members' => $members,
        ];
    }

    /**
     * @return array{error: int, assignment: string}
     */
    public static function parseSyncGroupResponse(Decoder $decoder): array
    {
        $decoder->int32(); // throttle time

        return [
            'error' => $decoder->int16(),
            'assignment' => $decoder->bytes() ?? '',
        ];
    }

    public static function parseHeartbeatResponse(Decoder $decoder): int
    {
        $decoder->int32(); // throttle time

        return $decoder->int16();
    }

    public static function parseLeaveGroupResponse(Decoder $decoder): int
    {
        $decoder->int32(); // throttle time

        return $decoder->int16();
    }

    /**
     * @return array<string, array<int, int>> topic => partition => error code
     */
    public static function parseOffsetCommitResponse(Decoder $decoder): array
    {
        $topics = [];
        $topicCount = $decoder->arrayLength() ?? 0;
        for ($i = 0; $i < $topicCount; $i++) {
            $name = $decoder->string();
            $partitionCount = $decoder->arrayLength() ?? 0;
            for ($p = 0; $p < $partitionCount; $p++) {
                $index = $decoder->int32();
                $topics[$name][$index] = $decoder->int16();
            }
        }

        return $topics;
    }

    /**
     * @return array{error: int, topics: array<string, array<int, array{offset: int, error: int}>>}
     */
    public static function parseOffsetFetchResponse(Decoder $decoder): array
    {
        $topics = [];
        $topicCount = $decoder->arrayLength() ?? 0;
        for ($i = 0; $i < $topicCount; $i++) {
            $name = $decoder->string();
            $partitionCount = $decoder->arrayLength() ?? 0;
            for ($p = 0; $p < $partitionCount; $p++) {
                $index = $decoder->int32();
                $offset = $decoder->int64();
                $decoder->nullableString(); // metadata
                $error = $decoder->int16();
                $topics[$name][$index] = ['offset' => $offset, 'error' => $error];
            }
        }

        return ['error' => $decoder->int16(), 'topics' => $topics];
    }
}
