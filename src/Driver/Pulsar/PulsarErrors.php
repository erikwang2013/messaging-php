<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Pulsar;

use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\MessagingException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;

/**
 * Pulsar ServerError names and exception mapping.
 */
final class PulsarErrors
{
    private const NAMES = [
        0 => 'UnknownError',
        1 => 'MetadataError',
        2 => 'PersistenceError',
        3 => 'AuthenticationError',
        4 => 'AuthorizationError',
        5 => 'ConsumerBusy',
        6 => 'ServiceNotReady',
        7 => 'ProducerBlockedQuotaExceededError',
        8 => 'ProducerBlockedQuotaExceededException',
        9 => 'ChecksumError',
        10 => 'UnsupportedVersionError',
        11 => 'TopicNotFound',
        12 => 'SubscriptionNotFound',
        13 => 'ConsumerNotFound',
        14 => 'TooManyRequests',
        15 => 'TopicTerminatedError',
        16 => 'ProducerBusy',
        17 => 'InvalidTopicName',
        18 => 'IncompatibleSchema',
        19 => 'ConsumerAssignError',
        20 => 'TransactionCoordinatorNotFound',
        21 => 'InvalidTxnStatus',
        22 => 'NotAllowedError',
        23 => 'TransactionConflict',
        24 => 'TransactionNotFound',
        25 => 'ProducerFenced',
    ];

    public static function name(int $code): string
    {
        return self::NAMES[$code] ?? sprintf('UnknownError_%d', $code);
    }

    public static function exception(int $code, string $message, string $context): MessagingException
    {
        $detail = sprintf(
            'Pulsar %s failed: %s (%d)%s.',
            $context,
            self::name($code),
            $code,
            $message !== '' ? ': ' . $message : '',
        );

        if ($code === 3 || $code === 4) {
            return new AuthenticationException($detail);
        }

        if ($code === 10) {
            return new UnsupportedFeatureException($detail);
        }

        return new ProtocolException($detail);
    }
}
