<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Redis;

use Erikwang2013\Messaging\Driver\Redis\Resp\RespParser;
use Erikwang2013\Messaging\Exception\ProtocolException;
use PHPUnit\Framework\TestCase;

final class RespParserTest extends TestCase
{
    public function test_parses_simple_string(): void
    {
        self::assertSame('OK', $this->parse("+OK\r\n"));
    }

    public function test_parses_integer(): void
    {
        self::assertSame(42, $this->parse(":42\r\n"));
    }

    public function test_parses_negative_integer(): void
    {
        self::assertSame(-1, $this->parse(":-1\r\n"));
    }

    public function test_parses_bulk_string(): void
    {
        self::assertSame('hello', $this->parse("$5\r\nhello\r\n"));
    }

    public function test_parses_binary_bulk_string(): void
    {
        self::assertSame("\x00\xff", $this->parse("$2\r\n\x00\xff\r\n"));
    }

    public function test_parses_null_bulk_string(): void
    {
        self::assertNull($this->parse("$-1\r\n"));
    }

    public function test_parses_array(): void
    {
        self::assertSame(['foo', 7], $this->parse("*2\r\n$3\r\nfoo\r\n:7\r\n"));
    }

    public function test_parses_nested_array_with_null_bulk(): void
    {
        self::assertSame(
            [['bar'], null],
            $this->parse("*2\r\n*1\r\n$3\r\nbar\r\n$-1\r\n"),
        );
    }

    public function test_parses_null_array(): void
    {
        self::assertNull($this->parse("*-1\r\n"));
    }

    public function test_error_reply_throws_protocol_exception(): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('WRONGTYPE Operation against a key');

        $this->parse("-WRONGTYPE Operation against a key\r\n");
    }

    public function test_unexpected_type_byte_throws(): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Unexpected RESP type byte [!]');

        $this->parse("!broken\r\n");
    }

    public function test_malformed_integer_throws(): void
    {
        $this->expectException(ProtocolException::class);

        $this->parse(":not-a-number\r\n");
    }

    public function test_negative_bulk_length_throws(): void
    {
        $this->expectException(ProtocolException::class);

        $this->parse("$-2\r\n");
    }

    public function test_missing_crlf_throws(): void
    {
        $this->expectException(ProtocolException::class);

        $this->parse('+OK');
    }

    public function test_bulk_string_without_crlf_terminator_throws(): void
    {
        $this->expectException(ProtocolException::class);

        $this->parse("$3\r\nfooXX");
    }

    public function test_empty_reply_throws(): void
    {
        $this->expectException(ProtocolException::class);

        $this->parse("\r\n");
    }

    private function parse(string $data): mixed
    {
        $offset = 0;

        $parser = new RespParser(
            static function () use ($data, &$offset): string {
                $end = strpos($data, "\r\n", $offset);
                if ($end === false) {
                    return '';
                }

                $line = substr($data, $offset, $end - $offset + 2);
                $offset = $end + 2;

                return $line;
            },
            static function (int $length) use ($data, &$offset): string {
                $chunk = substr($data, $offset, $length);
                $offset += strlen($chunk);

                return $chunk;
            },
        );

        return $parser->parse();
    }
}
