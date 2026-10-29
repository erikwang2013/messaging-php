<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

/**
 * CRC-32C (Castagnoli) used by Kafka record batch v2.
 */
final class Crc32c
{
    private const POLYNOMIAL = 0x82f63b78;

    /** @var array<int, int>|null */
    private static ?array $table = null;

    /**
     * Uses ext-hash's crc32c on PHP >= 8.1 and the table-driven fallback otherwise.
     */
    public static function compute(string $data): int
    {
        if (in_array('crc32c', hash_algos(), true)) {
            $raw = hash('crc32c', $data, true);
            $value = unpack('N', $raw);
            if ($value !== false) {
                return (int) $value[1];
            }
        }

        return self::computeFallback($data);
    }

    /**
     * Pure-PHP implementation, also used by tests to prove PHP 8.0 parity.
     */
    public static function computeFallback(string $data): int
    {
        $table = self::table();
        $crc = 0xffffffff;

        $length = strlen($data);
        for ($i = 0; $i < $length; $i++) {
            $crc = ($crc >> 8) ^ $table[($crc ^ ord($data[$i])) & 0xff];
        }

        return ($crc ^ 0xffffffff) & 0xffffffff;
    }

    /**
     * @return array<int, int>
     */
    private static function table(): array
    {
        if (self::$table !== null) {
            return self::$table;
        }

        $table = [];
        for ($index = 0; $index < 256; $index++) {
            $crc = $index;
            for ($bit = 0; $bit < 8; $bit++) {
                $crc = ($crc & 1) !== 0
                    ? self::POLYNOMIAL ^ ($crc >> 1)
                    : $crc >> 1;
            }

            $table[$index] = $crc;
        }

        return self::$table = $table;
    }
}
