<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

/**
 * Kafka's default partitioner hash (murmur2, seed 0x9747b28c).
 */
final class Murmur2
{
    private const SEED = 0x9747b28c;
    private const M = 0x5bd1e995;

    /**
     * Returns an unsigned 32-bit hash, matching org.apache.kafka.common.utils.Utils#murmur2.
     */
    public static function hash(string $key): int
    {
        $length = strlen($key);
        $h = (self::SEED ^ $length) & 0xffffffff;

        $position = 0;
        while ($length - $position >= 4) {
            $k = ord($key[$position])
                | (ord($key[$position + 1]) << 8)
                | (ord($key[$position + 2]) << 16)
                | (ord($key[$position + 3]) << 24);

            $k = ($k * self::M) & 0xffffffff;
            $k ^= ($k >> 24) & 0xffffffff;
            $k = ($k * self::M) & 0xffffffff;

            $h = (($h * self::M) & 0xffffffff) ^ $k;
            $position += 4;
        }

        $remaining = $length - $position;
        if ($remaining === 3) {
            $h ^= ord($key[$position + 2]) << 16;
        }
        if ($remaining >= 2) {
            $h ^= ord($key[$position + 1]) << 8;
        }
        if ($remaining >= 1) {
            $h ^= ord($key[$position]);
            $h = ($h * self::M) & 0xffffffff;
        }

        $h ^= ($h >> 13) & 0xffffffff;
        $h = ($h * self::M) & 0xffffffff;
        $h ^= ($h >> 15) & 0xffffffff;

        return $h & 0xffffffff;
    }

    public static function partition(string $key, int $partitionCount): int
    {
        if ($partitionCount < 1) {
            throw new \InvalidArgumentException('Kafka partition count must be positive.');
        }

        return (self::hash($key) & 0x7fffffff) % $partitionCount;
    }
}
