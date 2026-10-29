<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Pulsar;

use Erikwang2013\Messaging\Driver\Pulsar\Protobuf;
use Erikwang2013\Messaging\Exception\ProtocolException;
use PHPUnit\Framework\TestCase;

final class ProtobufTest extends TestCase
{
    public function test_varint_encoding_matches_protobuf_vectors(): void
    {
        self::assertSame("\x00", Protobuf::varint(0));
        self::assertSame("\x01", Protobuf::varint(1));
        self::assertSame("\x7f", Protobuf::varint(127));
        self::assertSame("\x80\x01", Protobuf::varint(128));
        self::assertSame("\xac\x02", Protobuf::varint(300));
        self::assertSame("\x80\x80\x01", Protobuf::varint(16384));
    }

    public function test_varint_rejects_negative_values(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        Protobuf::varint(-1);
    }

    public function test_varint_decoding_reports_the_next_offset(): void
    {
        self::assertSame([300, 2], Protobuf::readVarint("\xac\x02\xff", 0));
        self::assertSame([1, 4], Protobuf::readVarint("\x00\x00\x00\x01", 3));
    }

    public function test_tag_combines_field_number_and_wire_type(): void
    {
        self::assertSame("\x08", Protobuf::tag(1, Protobuf::WIRE_VARINT));
        self::assertSame("\x12", Protobuf::tag(2, Protobuf::WIRE_BYTES));
        self::assertSame("\x1d", Protobuf::tag(3, Protobuf::WIRE_FIXED32));
        self::assertSame("\x21", Protobuf::tag(4, Protobuf::WIRE_FIXED64));
    }

    public function test_fields_roundtrip_through_decode(): void
    {
        $encoded = Protobuf::varintField(2, 300)
            . Protobuf::stringField(4, 'hello')
            . Protobuf::boolField(9, true)
            . Protobuf::varintField(2, 7);

        self::assertSame([
            2 => [300, 7],
            4 => ['hello'],
            9 => [1],
        ], Protobuf::decode($encoded));
    }

    public function test_zero_length_bytes_field_roundtrips(): void
    {
        self::assertSame([1 => [0], 3 => ['']], Protobuf::decode(Protobuf::varintField(1, 0) . Protobuf::bytesField(3, '')));
    }

    public function test_fixed32_and_fixed64_fields_are_decoded(): void
    {
        $encoded = Protobuf::tag(1, Protobuf::WIRE_FIXED32) . pack('V', 0x01020304)
            . Protobuf::tag(2, Protobuf::WIRE_FIXED64) . pack('P', 42);

        self::assertSame([1 => [0x01020304], 2 => [42]], Protobuf::decode($encoded));
    }

    public function test_truncated_varint_throws(): void
    {
        $this->expectException(ProtocolException::class);

        Protobuf::decode("\x08\x80");
    }

    public function test_overlong_varint_throws(): void
    {
        $this->expectException(ProtocolException::class);

        Protobuf::decode("\x08" . str_repeat("\xff", 10) . "\x01");
    }

    public function test_truncated_bytes_field_throws(): void
    {
        $this->expectException(ProtocolException::class);

        Protobuf::decode("\x12\x05abc");
    }

    public function test_truncated_fixed_field_throws(): void
    {
        $this->expectException(ProtocolException::class);

        Protobuf::decode("\x0d\x01\x02");
    }

    public function test_field_zero_is_rejected(): void
    {
        $this->expectException(ProtocolException::class);

        Protobuf::decode("\x00\x01");
    }

    public function test_unsupported_wire_type_is_rejected(): void
    {
        $this->expectException(ProtocolException::class);

        Protobuf::decode("\x0b");
    }
}
