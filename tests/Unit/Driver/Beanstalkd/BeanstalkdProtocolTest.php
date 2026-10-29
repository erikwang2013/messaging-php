<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Beanstalkd;

use Erikwang2013\Messaging\Driver\Beanstalkd\BeanstalkdProtocol;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\RequestException;
use PHPUnit\Framework\TestCase;

final class BeanstalkdProtocolTest extends TestCase
{
    public function test_put_command_encodes_length_prefixed_body(): void
    {
        self::assertSame(
            "put 1024 0 60 5\r\nhello\r\n",
            BeanstalkdProtocol::putCommand(1024, 0, 60, 'hello'),
        );
    }

    public function test_put_command_uses_byte_length_for_binary_bodies(): void
    {
        self::assertSame(
            "put 0 2 30 4\r\n\x00\xffab\r\n",
            BeanstalkdProtocol::putCommand(0, 2, 30, "\x00\xffab"),
        );
    }

    public function test_delay_converts_milliseconds_up_to_whole_seconds(): void
    {
        self::assertSame(0, BeanstalkdProtocol::delaySeconds(null));
        self::assertSame(0, BeanstalkdProtocol::delaySeconds(0));
        self::assertSame(1, BeanstalkdProtocol::delaySeconds(1));
        self::assertSame(1, BeanstalkdProtocol::delaySeconds(1000));
        self::assertSame(2, BeanstalkdProtocol::delaySeconds(1001));
    }

    public function test_timeout_zero_blocks_and_positive_values_round_up(): void
    {
        self::assertSame(0, BeanstalkdProtocol::timeoutSeconds(0));
        self::assertSame(1, BeanstalkdProtocol::timeoutSeconds(1));
        self::assertSame(1, BeanstalkdProtocol::timeoutSeconds(1000));
        self::assertSame(2, BeanstalkdProtocol::timeoutSeconds(1001));
    }

    public function test_parse_line_splits_status_and_arguments(): void
    {
        self::assertSame(['INSERTED', ['7']], BeanstalkdProtocol::parseLine("INSERTED 7\r\n"));
        self::assertSame(['RESERVED', ['7', '11']], BeanstalkdProtocol::parseLine("RESERVED 7 11\r\n"));
        self::assertSame(['TIMED_OUT', []], BeanstalkdProtocol::parseLine("TIMED_OUT\r\n"));
        self::assertSame(['USING', ['tube-1']], BeanstalkdProtocol::parseLine("USING tube-1\r\n"));
    }

    public function test_parse_line_rejects_empty_line(): void
    {
        $this->expectException(ProtocolException::class);

        BeanstalkdProtocol::parseLine("\r\n");
    }

    public function test_assert_status_accepts_expected_status(): void
    {
        $this->expectNotToPerformAssertions();

        BeanstalkdProtocol::assertStatus('DELETED', 'DELETED', 'delete');
    }

    public function test_assert_status_maps_broker_errors_to_request_exception(): void
    {
        $this->expectException(RequestException::class);
        $this->expectExceptionMessage('NOT_FOUND');

        BeanstalkdProtocol::assertStatus('DELETED', 'NOT_FOUND', 'delete');
    }

    public function test_assert_status_maps_protocol_violations_to_protocol_exception(): void
    {
        $this->expectException(ProtocolException::class);

        BeanstalkdProtocol::assertStatus('INSERTED', 'BAD_FORMAT', 'put');
    }

    public function test_assert_status_maps_unknown_status_to_protocol_exception(): void
    {
        $this->expectException(ProtocolException::class);

        BeanstalkdProtocol::assertStatus('DELETED', 'WHAT', 'delete');
    }

    public function test_parse_stats_reads_yaml_body(): void
    {
        $stats = BeanstalkdProtocol::parseStats("---\nid: 5\npri: 1024\nreserves: 2\n");

        self::assertSame('5', $stats['id']);
        self::assertSame('1024', $stats['pri']);
        self::assertSame('2', $stats['reserves']);
    }
}
