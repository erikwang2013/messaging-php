<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

use Erikwang2013\Messaging\Exception\ProtocolException;

/**
 * Kafka protocol error code names and retry classification.
 */
final class KafkaErrors
{
    private const NAMES = [
        0 => 'NONE',
        1 => 'OFFSET_OUT_OF_RANGE',
        2 => 'CORRUPT_MESSAGE',
        3 => 'UNKNOWN_TOPIC_OR_PARTITION',
        5 => 'LEADER_NOT_AVAILABLE',
        6 => 'NOT_LEADER_OR_FOLLOWER',
        7 => 'REQUEST_TIMED_OUT',
        10 => 'MESSAGE_TOO_LARGE',
        13 => 'NETWORK_EXCEPTION',
        14 => 'COORDINATOR_LOAD_IN_PROGRESS',
        15 => 'COORDINATOR_NOT_AVAILABLE',
        16 => 'NOT_COORDINATOR',
        17 => 'INVALID_TOPIC_EXCEPTION',
        18 => 'RECORD_LIST_TOO_LARGE',
        19 => 'NOT_ENOUGH_REPLICAS',
        20 => 'NOT_ENOUGH_REPLICAS_AFTER_APPEND',
        21 => 'INVALID_REQUIRED_ACKS',
        22 => 'ILLEGAL_GENERATION',
        23 => 'INCONSISTENT_GROUP_PROTOCOL',
        24 => 'INVALID_GROUP_ID',
        25 => 'UNKNOWN_MEMBER_ID',
        26 => 'INVALID_SESSION_TIMEOUT',
        27 => 'REBALANCE_IN_PROGRESS',
        28 => 'INVALID_COMMIT_OFFSET_SIZE',
        29 => 'TOPIC_AUTHORIZATION_FAILED',
        30 => 'GROUP_AUTHORIZATION_FAILED',
        31 => 'CLUSTER_AUTHORIZATION_FAILED',
        35 => 'UNSUPPORTED_VERSION',
        36 => 'TOPIC_ALREADY_EXISTS',
        37 => 'INVALID_PARTITIONS',
        38 => 'INVALID_REPLICATION_FACTOR',
        42 => 'INVALID_REQUEST',
        43 => 'UNSUPPORTED_FOR_MESSAGE_FORMAT',
        46 => 'INVALID_PRODUCER_EPOCH',
        47 => 'INVALID_TXN_STATE',
        48 => 'INVALID_PRODUCER_ID_MAPPING',
        49 => 'INVALID_TRANSACTION_TIMEOUT',
        51 => 'INVALID_RECORD',
        56 => 'KAFKA_STORAGE_ERROR',
        57 => 'LOG_DIR_NOT_FOUND',
        59 => 'UNKNOWN_PRODUCER_ID',
        74 => 'FENCED_LEADER_EPOCH',
        75 => 'UNKNOWN_LEADER_EPOCH',
        79 => 'MEMBER_ID_REQUIRED',
        -1 => 'UNKNOWN_SERVER_ERROR',
    ];

    public static function name(int $code): string
    {
        return self::NAMES[$code] ?? sprintf('UNKNOWN_ERROR_%d', $code);
    }

    public static function isRetriable(int $code): bool
    {
        return in_array($code, [3, 5, 6, 7, 13, 14, 15, 16, 19, 20, 56, 74, 75], true);
    }

    public static function isGroupStateError(int $code): bool
    {
        return in_array($code, [22, 25, 27], true);
    }

    public static function assertNone(int $code, string $context): void
    {
        if ($code === 0) {
            return;
        }

        throw new ProtocolException(sprintf(
            'Kafka %s failed: %s (%d).',
            $context,
            self::name($code),
            $code,
        ));
    }
}
