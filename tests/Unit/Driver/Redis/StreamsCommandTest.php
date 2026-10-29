<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Redis;

use Erikwang2013\Messaging\Driver\Redis\StreamsCommand;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class StreamsCommandTest extends TestCase
{
    public function test_creates_group_with_mkstream(): void
    {
        self::assertSame(
            ['XGROUP', 'CREATE', 'orders', 'workers', '0', 'MKSTREAM'],
            StreamsCommand::createGroup('orders', 'workers'),
        );
    }

    public function test_add_builds_minimal_entry(): void
    {
        self::assertSame(
            ['XADD', 'orders', '*', 'body', 'hello'],
            StreamsCommand::add('orders', 'hello', [], null, null),
        );
    }

    public function test_add_includes_max_length_headers_and_key(): void
    {
        $command = StreamsCommand::add(
            'orders',
            'hello',
            ['trace' => 'abc', 'attempt' => 2],
            'key-1',
            1000,
        );

        self::assertSame('XADD', $command[0]);
        self::assertSame('orders', $command[1]);
        self::assertSame(['MAXLEN', '~', 1000], array_slice($command, 2, 3));
        self::assertSame(['*', 'body', 'hello'], array_slice($command, 5, 3));

        $headersIndex = array_search('headers', $command, true);
        self::assertIsInt($headersIndex);
        self::assertSame(['trace' => 'abc', 'attempt' => 2], json_decode((string) $command[$headersIndex + 1], true));
        self::assertSame(
            ['key', 'key-1'],
            array_slice($command, $headersIndex + 2, 2),
        );
    }

    public function test_add_preserves_binary_body(): void
    {
        $body = "\x00\x01\xffbinary";
        $command = StreamsCommand::add('orders', $body, [], null, null);

        self::assertSame($body, $command[4]);
    }

    public function test_add_omits_headers_field_when_empty(): void
    {
        self::assertNotContains('headers', StreamsCommand::add('orders', 'hello', [], 'k', null));
    }

    public function test_add_omits_key_field_when_null(): void
    {
        self::assertNotContains('key', StreamsCommand::add('orders', 'hello', ['a' => 1], null, null));
    }

    public function test_add_rejects_non_json_serializable_headers(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        StreamsCommand::add('orders', 'hello', ['bad' => "\xB1\x31"], null, null);
    }

    public function test_read_group_maps_timeouts_to_block_milliseconds(): void
    {
        self::assertSame(
            ['XREADGROUP', 'GROUP', 'workers', 'consumer-1', 'COUNT', 1, 'BLOCK', 250, 'STREAMS', 'orders', '>'],
            StreamsCommand::readGroup('orders', 'workers', 'consumer-1', 250),
        );

        self::assertSame(
            0,
            StreamsCommand::readGroup('orders', 'workers', 'consumer-1', 0)[7],
        );
    }

    public function test_ack_targets_stream_group_and_id(): void
    {
        self::assertSame(
            ['XACK', 'orders', 'workers', '1700000000000-0'],
            StreamsCommand::ack('orders', 'workers', '1700000000000-0'),
        );
    }
}
