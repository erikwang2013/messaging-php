<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;

/**
 * Kafka record batch (magic 2) encoder/decoder.
 *
 * Record shape: ['key' => ?string, 'value' => ?string, 'headers' => array<string, ?string>, 'timestamp' => int]
 * Decoded records also carry 'offset' and 'timestamp'.
 */
final class RecordBatch
{
    private const MAGIC = 2;
    private const HEADER_SIZE = 61;
    private const COMPRESSION_NONE = 0;
    private const COMPRESSION_GZIP = 1;
    private const ATTRIBUTES_CONTROL = 0x20;

    /**
     * @param list<array{key: ?string, value: ?string, headers: array<string, ?string>, timestamp: int}> $records
     */
    public static function encode(array $records, int $compression = self::COMPRESSION_NONE): string
    {
        if ($records === []) {
            throw new \InvalidArgumentException('A Kafka record batch needs at least one record.');
        }

        $baseTimestamp = $records[0]['timestamp'];
        $maxTimestamp = $baseTimestamp;
        foreach ($records as $record) {
            $baseTimestamp = min($baseTimestamp, $record['timestamp']);
            $maxTimestamp = max($maxTimestamp, $record['timestamp']);
        }

        $payload = '';
        foreach ($records as $index => $record) {
            $payload .= self::encodeRecord($record, $index, $baseTimestamp);
        }

        $attributes = $compression;
        if ($compression === self::COMPRESSION_GZIP) {
            if (!function_exists('gzencode')) {
                throw new UnsupportedFeatureException('Kafka gzip compression requires the zlib extension.');
            }

            $compressed = gzencode($payload, 6);
            if ($compressed === false) {
                throw new ProtocolException('Unable to gzip-compress a Kafka record batch.');
            }

            $payload = $compressed;
        } elseif ($compression !== self::COMPRESSION_NONE) {
            throw new UnsupportedFeatureException(sprintf('Kafka compression codec [%d] is not supported.', $compression));
        }

        $crcCovered = (new Encoder())
            ->int16($attributes)
            ->int32(count($records) - 1)
            ->int64($baseTimestamp)
            ->int64($maxTimestamp)
            ->int64(-1)
            ->int16(-1)
            ->int32(-1)
            ->int32(count($records))
            ->raw($payload)
            ->toString();

        $batchBody = (new Encoder())
            ->int32(-1)
            ->int8(self::MAGIC)
            ->int32(Crc32c::compute($crcCovered))
            ->raw($crcCovered)
            ->toString();

        return (new Encoder())
            ->int64(0)
            ->int32(strlen($batchBody))
            ->raw($batchBody)
            ->toString();
    }

    /**
     * @return list<array{offset: int, timestamp: int, key: ?string, value: ?string, headers: array<string, ?string>}>
     */
    public static function decode(string $data, int $requestOffset = 0, int $maxRecords = PHP_INT_MAX): array
    {
        $records = [];
        $decoder = new Decoder($data);

        while ($decoder->remaining() >= 12) {
            $batchStart = $decoder->position();
            $baseOffset = $decoder->int64();
            $batchLength = $decoder->int32();

            if ($batchLength < self::HEADER_SIZE - 12) {
                throw new ProtocolException(sprintf('Invalid Kafka record batch length [%d].', $batchLength));
            }

            // Brokers can return a truncated tail batch under tight fetch limits.
            if ($decoder->remaining() < $batchLength) {
                break;
            }

            $batch = substr($data, $batchStart + 12, $batchLength);

            $read = new Decoder($batch);
            $read->int32(); // partition leader epoch
            if ($read->int8() !== self::MAGIC) {
                throw new ProtocolException('Unsupported Kafka record batch magic; expected magic 2.');
            }

            $storedCrc = $read->int32() & 0xffffffff;
            $actualCrc = Crc32c::compute(substr($batch, 9));
            if ($storedCrc !== $actualCrc) {
                throw new ProtocolException(sprintf(
                    'Kafka record batch CRC32C mismatch (stored %08x, actual %08x).',
                    $storedCrc,
                    $actualCrc,
                ));
            }

            $attributes = $read->int16();
            $read->int32(); // last offset delta
            $baseTimestamp = $read->int64();
            $read->int64(); // max timestamp
            $read->int64(); // producer id
            $read->int16(); // producer epoch
            $read->int32(); // base sequence
            $recordCount = $read->int32();
            $payload = $read->rest();

            $compression = $attributes & 0x07;
            if ($compression !== self::COMPRESSION_NONE) {
                if ($compression !== self::COMPRESSION_GZIP) {
                    throw new UnsupportedFeatureException(sprintf(
                        'Kafka compression codec [%d] is not supported; only gzip is implemented.',
                        $compression,
                    ));
                }

                $decompressed = @gzdecode($payload);
                if ($decompressed === false) {
                    throw new ProtocolException('Unable to gunzip a Kafka record batch.');
                }

                $payload = $decompressed;
            }

            $decoder->skip($batchLength);

            if (($attributes & self::ATTRIBUTES_CONTROL) !== 0) {
                continue;
            }

            $readRecords = self::decodeRecords($payload, $recordCount, $baseOffset, $baseTimestamp, $requestOffset, $maxRecords - count($records));
            foreach ($readRecords as $record) {
                $records[] = $record;
            }

            if (count($records) >= $maxRecords) {
                break;
            }
        }

        return $records;
    }

    /**
     * @param array{key: ?string, value: ?string, headers: array<string, ?string>, timestamp: int} $record
     */
    private static function encodeRecord(array $record, int $index, int $baseTimestamp): string
    {
        $key = $record['key'];
        $value = $record['value'];

        $body = (new Encoder())
            ->int8(0)
            ->varlong($record['timestamp'] - $baseTimestamp)
            ->varint($index)
            ->varint($key === null ? -1 : strlen($key))
            ->raw($key ?? '')
            ->varint($value === null ? -1 : strlen($value))
            ->raw($value ?? '')
            ->varint(count($record['headers']));

        foreach ($record['headers'] as $name => $headerValue) {
            $name = (string) $name;
            $body->varint(strlen($name))->raw($name);
            if ($headerValue === null) {
                $body->varint(-1);
            } else {
                $body->varint(strlen($headerValue))->raw($headerValue);
            }
        }

        $recordBody = $body->toString();

        return (new Encoder())->varint(strlen($recordBody))->raw($recordBody)->toString();
    }

    /**
     * @return list<array{offset: int, timestamp: int, key: ?string, value: ?string, headers: array<string, ?string>}>
     */
    private static function decodeRecords(
        string $payload,
        int $recordCount,
        int $baseOffset,
        int $baseTimestamp,
        int $requestOffset,
        int $maxRecords,
    ): array {
        if ($maxRecords <= 0) {
            return [];
        }

        $records = [];
        $decoder = new Decoder($payload);

        for ($index = 0; $index < $recordCount && $decoder->remaining() > 0; $index++) {
            $recordLength = $decoder->varint();
            if ($recordLength < 0) {
                throw new ProtocolException('Kafka record length is negative.');
            }

            $record = new Decoder($decoder->take($recordLength));
            $record->int8(); // record attributes (unused)

            $timestampDelta = $record->varlong();
            $offsetDelta = $record->varint();
            $keyLength = $record->varint();
            $key = $keyLength === -1 ? null : $record->take($keyLength);
            $valueLength = $record->varint();
            $value = $valueLength === -1 ? null : $record->take($valueLength);

            $headerCount = $record->varint();
            $headers = [];
            for ($header = 0; $header < $headerCount; $header++) {
                $nameLength = $record->varint();
                if ($nameLength < 0) {
                    throw new ProtocolException('Kafka record header name is null.');
                }

                $name = $record->take($nameLength);
                $headerValueLength = $record->varint();
                $headers[$name] = $headerValueLength === -1 ? null : $record->take($headerValueLength);
            }

            $offset = $baseOffset + $offsetDelta;
            if ($offset < $requestOffset) {
                continue;
            }

            $records[] = [
                'offset' => $offset,
                'timestamp' => $baseTimestamp + $timestampDelta,
                'key' => $key,
                'value' => $value,
                'headers' => $headers,
            ];

            if (count($records) >= $maxRecords) {
                break;
            }
        }

        return $records;
    }
}
