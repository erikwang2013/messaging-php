<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Kafka;

use Erikwang2013\Messaging\Driver\Kafka\Crc32c;
use Erikwang2013\Messaging\Driver\Kafka\RecordBatch;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class RecordBatchTest extends TestCase
{
    private const GOLDEN_SINGLE_RECORD = '00000000000000000000003dffffffff02e641a44b'
        . '0000'
        . '00000000'
        . '0000018bcfe56800'
        . '0000018bcfe56800'
        . 'ffffffffffffffff'
        . 'ffff'
        . 'ffffffff'
        . '00000001'
        . '16'
        . '000000010a68656c6c6f00';

    public function test_golden_single_record_batch_bytes(): void
    {
        $batch = RecordBatch::encode([[
            'key' => null,
            'value' => 'hello',
            'headers' => [],
            'timestamp' => 1700000000000,
        ]]);

        self::assertSame(self::GOLDEN_SINGLE_RECORD, bin2hex($batch));
    }

    public function test_roundtrip_preserves_keys_headers_and_binary_values(): void
    {
        $records = [
            [
                'key' => 'k1',
                'value' => "bin\x00\xff",
                'headers' => ['h1' => 'v1', 'empty' => null],
                'timestamp' => 1700000000100,
            ],
            [
                'key' => null,
                'value' => 'second',
                'headers' => [],
                'timestamp' => 1700000000200,
            ],
        ];

        $decoded = RecordBatch::decode(RecordBatch::encode($records));

        self::assertCount(2, $decoded);
        self::assertSame(['offset' => 0, 'timestamp' => 1700000000100, 'key' => 'k1', 'value' => "bin\x00\xff", 'headers' => ['h1' => 'v1', 'empty' => null]], $decoded[0]);
        self::assertSame(1, $decoded[1]['offset']);
        self::assertNull($decoded[1]['key']);
        self::assertSame('second', $decoded[1]['value']);
    }

    public function test_decode_filters_offsets_and_caps_max_records(): void
    {
        $batch = RecordBatch::encode([
            ['key' => null, 'value' => 'a', 'headers' => [], 'timestamp' => 1000],
            ['key' => null, 'value' => 'b', 'headers' => [], 'timestamp' => 1001],
            ['key' => null, 'value' => 'c', 'headers' => [], 'timestamp' => 1002],
        ]);

        $all = RecordBatch::decode($batch, 0);
        self::assertSame([0, 1, 2], array_column($all, 'offset'));

        $filtered = RecordBatch::decode($batch, 1);
        self::assertSame([1, 2], array_column($filtered, 'offset'));

        $limited = RecordBatch::decode($batch, 0, 2);
        self::assertSame([0, 1], array_column($limited, 'offset'));

        $none = RecordBatch::decode($batch, 0, 0);
        self::assertSame([], $none);
    }

    public function test_gzip_roundtrip(): void
    {
        if (!function_exists('gzencode')) {
            self::markTestSkipped('ext-zlib is not installed.');
        }

        $records = [[
            'key' => 'zip',
            'value' => str_repeat('compress me ', 100),
            'headers' => [],
            'timestamp' => 1700000000000,
        ]];

        $batch = RecordBatch::encode($records, 1);
        self::assertSame(1, unpack('n', substr($batch, 21, 2))[1]);

        $decoded = RecordBatch::decode($batch);
        self::assertSame($records[0]['value'], $decoded[0]['value']);
    }

    public function test_unsupported_compression_is_rejected(): void
    {
        $this->expectException(UnsupportedFeatureException::class);
        $this->expectExceptionMessage('only gzip');

        RecordBatch::decode(self::withCompression(RecordBatch::encode([[
            'key' => null,
            'value' => 'hello',
            'headers' => [],
            'timestamp' => 1700000000000,
        ]]), 2));
    }

    public function test_unsupported_compression_on_encode_is_rejected(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        RecordBatch::encode([[
            'key' => null,
            'value' => 'hello',
            'headers' => [],
            'timestamp' => 1700000000000,
        ]], 2);
    }

    public function test_crc_mismatch_is_rejected(): void
    {
        $batch = RecordBatch::encode([[
            'key' => null,
            'value' => 'hello',
            'headers' => [],
            'timestamp' => 1700000000000,
        ]]);
        $batch[strlen($batch) - 1] = chr(ord($batch[strlen($batch) - 1]) ^ 0xff);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('CRC32C mismatch');

        RecordBatch::decode($batch);
    }

    public function test_truncated_tail_batch_is_ignored(): void
    {
        $batch = RecordBatch::encode([[
            'key' => null,
            'value' => 'hello',
            'headers' => [],
            'timestamp' => 1700000000000,
        ]]);

        self::assertSame([], RecordBatch::decode(substr($batch, 0, 10)));
    }

    public function test_empty_batch_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        RecordBatch::encode([]);
    }

    private static function withCompression(string $batch, int $codec): string
    {
        $batch[21] = chr(0);
        $batch[22] = chr($codec);
        $crc = Crc32c::compute(substr($batch, 21));

        return substr_replace($batch, pack('N', $crc), 17, 4);
    }
}
