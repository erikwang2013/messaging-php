<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Kafka;

use Erikwang2013\Messaging\Driver\Kafka\Crc32c;
use PHPUnit\Framework\TestCase;

final class Crc32cTest extends TestCase
{
    public function test_standard_vectors(): void
    {
        self::assertSame(0x00000000, Crc32c::compute(''));
        self::assertSame(0xE3069283, Crc32c::compute('123456789'));
        self::assertSame(0x22620404, Crc32c::compute('The quick brown fox jumps over the lazy dog'));
    }

    public function test_php_80_fallback_matches_native_hash(): void
    {
        $samples = [
            '',
            'a',
            '123456789',
            'The quick brown fox jumps over the lazy dog',
            "\x00\x01\xff\x80binary",
            str_repeat('kafka', 1000),
        ];

        for ($i = 1; $i <= 32; $i++) {
            $samples[] = random_bytes($i);
        }

        foreach ($samples as $sample) {
            self::assertSame(
                Crc32c::compute($sample),
                Crc32c::computeFallback($sample),
                'CRC32C mismatch for ' . bin2hex($sample),
            );
        }
    }
}
