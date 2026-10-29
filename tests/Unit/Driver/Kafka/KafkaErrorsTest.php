<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Kafka;

use Erikwang2013\Messaging\Driver\Kafka\KafkaErrors;
use Erikwang2013\Messaging\Exception\ProtocolException;
use PHPUnit\Framework\TestCase;

final class KafkaErrorsTest extends TestCase
{
    public function test_error_names(): void
    {
        self::assertSame('NONE', KafkaErrors::name(0));
        self::assertSame('OFFSET_OUT_OF_RANGE', KafkaErrors::name(1));
        self::assertSame('REBALANCE_IN_PROGRESS', KafkaErrors::name(27));
        self::assertSame('UNKNOWN_SERVER_ERROR', KafkaErrors::name(-1));
        self::assertSame('UNKNOWN_ERROR_12345', KafkaErrors::name(12345));
    }

    public function test_retriable_and_group_state_classification(): void
    {
        foreach ([3, 5, 6, 7, 13, 14, 15, 16, 19, 20, 56, 74, 75] as $code) {
            self::assertTrue(KafkaErrors::isRetriable($code), 'code ' . $code);
        }

        self::assertFalse(KafkaErrors::isRetriable(0));
        self::assertFalse(KafkaErrors::isRetriable(1));
        self::assertFalse(KafkaErrors::isRetriable(10));

        self::assertTrue(KafkaErrors::isGroupStateError(22));
        self::assertTrue(KafkaErrors::isGroupStateError(25));
        self::assertTrue(KafkaErrors::isGroupStateError(27));
        self::assertFalse(KafkaErrors::isGroupStateError(0));
    }

    public function test_assert_none_is_silent_for_success(): void
    {
        KafkaErrors::assertNone(0, 'Fetch');

        self::assertTrue(true);
    }

    public function test_assert_none_throws_with_error_name_and_context(): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Kafka Produce failed: NOT_LEADER_OR_FOLLOWER (6).');

        KafkaErrors::assertNone(6, 'Produce');
    }
}
