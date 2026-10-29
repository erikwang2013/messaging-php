<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Pulsar;

use Erikwang2013\Messaging\Exception\ProtocolException;

/**
 * Minimal protobuf wire codec for the Pulsar command subset.
 */
final class Protobuf
{
    public const WIRE_VARINT = 0;
    public const WIRE_FIXED64 = 1;
    public const WIRE_BYTES = 2;
    public const WIRE_FIXED32 = 5;

    public static function varint(int $value): string
    {
        if ($value < 0) {
            throw new \InvalidArgumentException('Pulsar protobuf varints must be non-negative.');
        }

        $encoded = '';
        while ($value > 0x7f) {
            $encoded .= chr(($value & 0x7f) | 0x80);
            $value >>= 7;
        }

        return $encoded . chr($value);
    }

    public static function tag(int $field, int $wireType): string
    {
        if ($field < 1) {
            throw new \InvalidArgumentException('Protobuf field numbers must be positive.');
        }

        return self::varint(($field << 3) | $wireType);
    }

    public static function varintField(int $field, int $value): string
    {
        return self::tag($field, self::WIRE_VARINT) . self::varint($value);
    }

    public static function boolField(int $field, bool $value): string
    {
        return self::varintField($field, $value ? 1 : 0);
    }

    public static function bytesField(int $field, string $value): string
    {
        return self::tag($field, self::WIRE_BYTES) . self::varint(strlen($value)) . $value;
    }

    public static function stringField(int $field, string $value): string
    {
        return self::bytesField($field, $value);
    }

    /**
     * @return array<int, list<int|string>>
     */
    public static function decode(string $data): array
    {
        $fields = [];
        $offset = 0;
        $length = strlen($data);

        while ($offset < $length) {
            [$key, $offset] = self::readVarint($data, $offset);
            $field = $key >> 3;
            $wireType = $key & 0x07;

            if ($field < 1) {
                throw new ProtocolException('Malformed protobuf field number 0.');
            }

            switch ($wireType) {
                case self::WIRE_VARINT:
                    [$value, $offset] = self::readVarint($data, $offset);
                    $fields[$field][] = $value;
                    break;

                case self::WIRE_FIXED64:
                    if ($offset + 8 > $length) {
                        throw new ProtocolException('Truncated protobuf fixed64 field.');
                    }
                    $unpacked = unpack('Pvalue', substr($data, $offset, 8));
                    $fields[$field][] = $unpacked === false ? 0 : (int) $unpacked['value'];
                    $offset += 8;
                    break;

                case self::WIRE_BYTES:
                    [$size, $offset] = self::readVarint($data, $offset);
                    if ($offset + $size > $length) {
                        throw new ProtocolException('Truncated protobuf length-delimited field.');
                    }
                    $fields[$field][] = substr($data, $offset, $size);
                    $offset += $size;
                    break;

                case self::WIRE_FIXED32:
                    if ($offset + 4 > $length) {
                        throw new ProtocolException('Truncated protobuf fixed32 field.');
                    }
                    $unpacked = unpack('Vvalue', substr($data, $offset, 4));
                    $fields[$field][] = $unpacked === false ? 0 : (int) $unpacked['value'];
                    $offset += 4;
                    break;

                default:
                    throw new ProtocolException(sprintf('Unsupported protobuf wire type [%d].', $wireType));
            }
        }

        return $fields;
    }

    /**
     * @return array{0: int, 1: int} value and the offset after it
     */
    public static function readVarint(string $data, int $offset): array
    {
        $value = 0;
        $shift = 0;
        $length = strlen($data);

        while (true) {
            if ($offset >= $length) {
                throw new ProtocolException('Truncated protobuf varint.');
            }

            $byte = ord($data[$offset]);
            $offset++;
            $value |= ($byte & 0x7f) << $shift;

            if (($byte & 0x80) === 0) {
                return [$value, $offset];
            }

            $shift += 7;
            if ($shift > 63) {
                throw new ProtocolException('Protobuf varint is longer than 64 bits.');
            }
        }
    }

    /**
     * @param array<int, list<int|string>> $fields
     */
    public static function intValue(array $fields, int $field, int $default = 0): int
    {
        $value = $fields[$field][0] ?? null;

        return is_int($value) ? $value : $default;
    }

    /**
     * @param array<int, list<int|string>> $fields
     */
    public static function stringValue(array $fields, int $field): ?string
    {
        $value = $fields[$field][0] ?? null;

        return is_string($value) ? $value : null;
    }
}
