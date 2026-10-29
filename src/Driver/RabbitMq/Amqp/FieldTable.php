<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RabbitMq\Amqp;

use Erikwang2013\Messaging\Exception\ProtocolException;

/**
 * AMQP 0-9-1 field tables, used for connection properties, queue arguments
 * and the basic content header's application_headers.
 */
final class FieldTable
{
    /**
     * @param array<string, mixed> $table
     */
    public static function encode(array $table): string
    {
        $entries = '';
        foreach ($table as $name => $value) {
            $entries .= Codec::shortstr((string) $name) . self::encodeValue($value);
        }

        return Codec::long(strlen($entries)) . $entries;
    }

    /**
     * @return array<string, mixed>
     */
    public static function decode(Reader $reader): array
    {
        $entries = new Reader($reader->read($reader->long()));
        $table = [];

        while ($entries->remaining() > 0) {
            $name = $entries->shortstr();
            $table[$name] = self::decodeValue($entries);
        }

        return $table;
    }

    private static function encodeValue(mixed $value): string
    {
        if ($value === null) {
            return 'V';
        }

        if (is_bool($value)) {
            return 't' . chr($value ? 1 : 0);
        }

        if (is_int($value)) {
            if ($value >= -2147483648 && $value <= 2147483647) {
                return 'I' . Codec::long($value);
            }

            return 'l' . Codec::longlong($value);
        }

        if (is_float($value)) {
            return 'd' . pack('E', $value);
        }

        if (is_string($value)) {
            return 'S' . Codec::longstr($value);
        }

        if (is_array($value)) {
            if (self::isList($value)) {
                $items = '';
                foreach ($value as $item) {
                    $items .= self::encodeValue($item);
                }

                return 'A' . Codec::long(strlen($items)) . $items;
            }

            return 'F' . self::encode($value);
        }

        throw new \InvalidArgumentException(sprintf(
            'Cannot encode a header value of type [%s] as an AMQP field value.',
            get_debug_type($value),
        ));
    }

    private static function decodeValue(Reader $reader): mixed
    {
        $type = $reader->read(1);

        switch ($type) {
            case 't':
                return $reader->octet() !== 0;
            case 'b':
                $value = $reader->octet();

                return $value >= 0x80 ? $value - 0x100 : $value;
            case 'B':
                return $reader->octet();
            case 's':
                $value = $reader->short();

                return $value >= 0x8000 ? $value - 0x10000 : $value;
            case 'u':
                return $reader->short();
            case 'I':
                $value = $reader->long();

                return $value >= 0x80000000 ? $value - 0x100000000 : $value;
            case 'i':
                return $reader->long();
            case 'l':
                return $reader->longlong();
            case 'f':
                return (float) unpack('G', $reader->read(4))[1];
            case 'd':
                return (float) unpack('E', $reader->read(8))[1];
            case 'D':
                $scale = $reader->octet();
                $value = $reader->long();

                return $value / (10 ** $scale);
            case 'S':
                return $reader->longstr();
            case 'x':
                return $reader->longstr();
            case 'A':
                $items = new Reader($reader->read($reader->long()));
                $list = [];
                while ($items->remaining() > 0) {
                    $list[] = self::decodeValue($items);
                }

                return $list;
            case 'T':
                return $reader->longlong();
            case 'F':
                return self::decode($reader);
            case 'V':
                return null;
            default:
                throw new ProtocolException(sprintf('Unknown AMQP field type [%s].', $type));
        }
    }

    /**
     * @param array<mixed> $value
     */
    private static function isList(array $value): bool
    {
        if ($value === []) {
            return true;
        }

        return array_keys($value) === range(0, count($value) - 1);
    }
}
