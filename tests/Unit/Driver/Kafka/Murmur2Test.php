<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Kafka;

use Erikwang2013\Messaging\Driver\Kafka\Murmur2;
use PHPUnit\Framework\TestCase;

final class Murmur2Test extends TestCase
{
    public function test_known_kafka_vectors(): void
    {
        $vectors = [
            ['21', 3321034988],
            ['foobar', 3504634814],
            ['a-little-bit-long-string', 3308985760],
            ['a-little-bit-longer-string', 2808662467],
            ['lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8', 4236069325],
        ];

        foreach ($vectors as [$key, $expected]) {
            self::assertSame($expected, Murmur2::hash($key), 'murmur2(' . $key . ')');
        }
    }

    public function test_partition_uses_positive_hash_modulo_count(): void
    {
        foreach (['', 'a', '21', 'partition-key', "\x00\xff"] as $key) {
            $expected = (Murmur2::hash($key) & 0x7fffffff) % 7;
            self::assertSame($expected, Murmur2::partition($key, 7));
        }
    }

    public function test_partition_count_must_be_positive(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        Murmur2::partition('key', 0);
    }
}
