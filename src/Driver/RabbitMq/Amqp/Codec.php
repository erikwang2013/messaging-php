<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RabbitMq\Amqp;

/**
 * Big-endian AMQP 0-9-1 wire primitives.
 */
final class Codec
{
    public static function shortstr(string $value): string
    {
        $length = strlen($value);
        if ($length > 255) {
            throw new \InvalidArgumentException(sprintf(
                'AMQP short strings are limited to 255 bytes, got %d.',
                $length,
            ));
        }

        return chr($length) . $value;
    }

    public static function longstr(string $value): string
    {
        return pack('N', strlen($value)) . $value;
    }

    /**
     * Packs booleans into octets, least significant bit first.
     *
     * @param list<bool> $bits
     */
    public static function bits(array $bits): string
    {
        $packed = '';
        $octet = 0;
        $position = 0;

        foreach ($bits as $bit) {
            if ($bit) {
                $octet |= 1 << $position;
            }

            $position++;
            if ($position === 8) {
                $packed .= chr($octet);
                $octet = 0;
                $position = 0;
            }
        }

        if ($position > 0) {
            $packed .= chr($octet);
        }

        return $packed;
    }

    public static function short(int $value): string
    {
        return pack('n', $value & 0xFFFF);
    }

    public static function long(int $value): string
    {
        return pack('N', $value & 0xFFFFFFFF);
    }

    public static function longlong(int $value): string
    {
        return pack('J', $value);
    }
}
