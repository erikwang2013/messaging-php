<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Kafka;

use Erikwang2013\Messaging\Driver\Kafka\Decoder;
use Erikwang2013\Messaging\Driver\Kafka\Encoder;
use Erikwang2013\Messaging\Exception\ProtocolException;
use PHPUnit\Framework\TestCase;

final class CodecTest extends TestCase
{
    public function test_zigzag_varint_known_encodings(): void
    {
        $vectors = [
            0 => '00',
            1 => '02',
            -1 => '01',
            2 => '04',
            -2 => '03',
            63 => '7e',
            64 => '8001',
            -64 => '7f',
            300 => 'd804',
            -300 => 'd704',
        ];

        foreach ($vectors as $value => $expected) {
            self::assertSame($expected, bin2hex((new Encoder())->varint($value)->toString()), 'varint ' . $value);
            self::assertSame($expected, bin2hex((new Encoder())->varlong($value)->toString()), 'varlong ' . $value);
            self::assertSame($value, (new Decoder(hex2bin($expected)))->varint(), 'decode ' . $value);
        }
    }

    public function test_uvarint_known_encoding(): void
    {
        self::assertSame('ac02', bin2hex((new Encoder())->uvarint(300)->toString()));
        self::assertSame(300, (new Decoder(hex2bin('ac02')))->uvarint());
        self::assertSame(0, (new Decoder(hex2bin('00')))->uvarint());
    }

    public function test_integers_roundtrip(): void
    {
        $values = [-1, 0, 1, 127, 128, -129, 1000000, -1000000, 1700000000000, -1700000000000];

        foreach ($values as $value) {
            self::assertSame($value, (new Decoder((new Encoder())->int64($value)->toString()))->int64());
        }

        self::assertSame(-1, (new Decoder(hex2bin('ffffffff')))->int32());
        self::assertSame(-129, (new Decoder(hex2bin('ffffff7f')))->int32());
        self::assertSame(-1, (new Decoder(hex2bin('ff')))->int8());
        self::assertTrue((new Decoder(hex2bin('01')))->bool());
        self::assertFalse((new Decoder(hex2bin('00')))->bool());
    }

    public function test_strings_bytes_and_compact_strings_roundtrip(): void
    {
        $decoder = new Decoder(
            (new Encoder())
                ->string('abc')
                ->nullableString(null)
                ->bytes(null)
                ->bytes('')
                ->compactString('hello')
                ->compactNullableString(null)
                ->compactNullableString('world')
                ->toString(),
        );

        self::assertSame('abc', $decoder->string());
        self::assertNull($decoder->nullableString());
        self::assertNull($decoder->bytes());
        self::assertSame('', $decoder->bytes());
        self::assertSame('hello', $decoder->compactString());
        self::assertNull($decoder->compactNullableString());
        self::assertSame('world', $decoder->compactNullableString());
        self::assertSame(0, $decoder->remaining());
    }

    public function test_tagged_fields_are_skipped(): void
    {
        $encoder = (new Encoder())
            ->uvarint(2)
            ->uvarint(0)
            ->uvarint(2)
            ->raw('xy')
            ->uvarint(1)
            ->uvarint(0)
            ->int8(42);

        $decoder = new Decoder($encoder->toString());
        $decoder->skipTags();

        self::assertSame(42, $decoder->int8());
    }

    public function test_truncated_input_throws(): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Truncated Kafka response');

        (new Decoder("\x00\x01"))->int32();
    }

    public function test_negative_string_length_throws(): void
    {
        $this->expectException(ProtocolException::class);

        (new Decoder(hex2bin('fffe')))->nullableString();
    }

    public function test_array_length_handles_null(): void
    {
        self::assertNull((new Decoder(hex2bin('ffffffff')))->arrayLength());
        self::assertSame(2, (new Decoder(hex2bin('00000002')))->arrayLength());
    }
}
