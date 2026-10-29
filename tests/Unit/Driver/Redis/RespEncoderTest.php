<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Redis;

use Erikwang2013\Messaging\Driver\Redis\Resp\RespEncoder;
use PHPUnit\Framework\TestCase;

final class RespEncoderTest extends TestCase
{
    public function test_encodes_command_as_bulk_string_array(): void
    {
        self::assertSame(
            "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            RespEncoder::encode(['SET', 'foo', 'bar']),
        );
    }

    public function test_encodes_integers_as_strings(): void
    {
        self::assertSame(
            "*2\r\n$6\r\nSELECT\r\n$1\r\n2\r\n",
            RespEncoder::encode(['SELECT', 2]),
        );
    }

    public function test_encodes_binary_body_with_byte_length(): void
    {
        $body = "\x00\x01\xff";

        self::assertSame(
            "*1\r\n$3\r\n" . $body . "\r\n",
            RespEncoder::encode([$body]),
        );
    }

    public function test_encodes_empty_command(): void
    {
        self::assertSame("*0\r\n", RespEncoder::encode([]));
    }
}
