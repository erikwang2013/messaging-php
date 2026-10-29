<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RocketMq;

use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;

/**
 * RocketMQ stored-message format and Java-properties encoding.
 */
final class RocketMqMessageCodec
{
    public const MAGIC_CODE = 0xDAA320A7;
    public const MAGIC_CODE_V2 = 0xDAA320AB;

    public const COMPRESSED_FLAG = 0x1;
    public const BORNHOST_V6_FLAG = 0x10;
    public const STOREHOST_V6_FLAG = 0x20;

    /** Broker-side maximum record size for a message without body/topic/properties. */
    private const MIN_RECORD_SIZE = 91;

    /** Default broker delay levels: 1s, 5s, 10s, 30s, 1m ... 2h. */
    private const DELAY_LEVELS_MS = [
        1000, 5000, 10000, 30000,
        60000, 120000, 180000, 240000, 300000, 360000, 420000, 480000, 540000, 600000,
        1200000, 1800000, 3600000, 7200000,
    ];

    /** Properties owned by RocketMQ (or by this driver) and hidden from user headers. */
    private const SYSTEM_PROPERTIES = [
        'KEYS', 'TAGS_SYSTEM', 'UNIQ_KEY', 'WAIT', 'DELAY', 'RETRY_TOPIC', 'REAL_TOPIC', 'REAL_QID',
        'CLUSTER', 'MSG_REGION', 'TRACE_ON', 'ORIGIN_MESSAGE_ID', 'RECONSUME_TIME', 'PGROUP',
        'TRANSFER_FLAG', 'CORRECTION_FLAG', 'MQ2_FLAG', 'MAX_RECONSUME_TIMES', 'CONSUME_START_TIME',
        'MIN_OFFSET', 'MAX_OFFSET', 'INNER_MULTI_DISPATCH', 'INNER_MULTI_QUEUE_OFFSET', 'TRAN_MSG',
        'TRAN_PREPARED_QUEUE_OFFSET', 'TRAN_CHECK_TIMES', 'CHECK_IMMUNITY_TIME_IN_SECONDS', 'BUYER_ID',
        'MSG_TYPE', 'TTL', 'ARRIVE_TIME', 'REPLY_TO_CLIENT', 'PUSH_REPLY_TIME', 'INSTANCE_ID',
        'CORRELATION_ID', 'MSG_REGION',
    ];

    private function __construct()
    {
    }

    /**
     * @throws \InvalidArgumentException when headers cannot be represented or collide with driver properties
     */
    public static function buildProperties(Message $message, string $uniqKey): string
    {
        $properties = [];

        foreach ($message->headers as $name => $value) {
            $name = (string) $name;
            if (in_array($name, self::SYSTEM_PROPERTIES, true)) {
                throw new \InvalidArgumentException(sprintf(
                    'Header [%s] is reserved by the RocketMQ driver; use Message::$key or Message::$delayMs instead.',
                    $name,
                ));
            }

            $properties[$name] = self::stringifyHeader($name, $value);
        }

        $properties['WAIT'] = 'true';
        $properties['UNIQ_KEY'] = $uniqKey;

        if ($message->key !== null && $message->key !== '') {
            $properties['KEYS'] = $message->key;
        }

        if ($message->delayMs !== null && $message->delayMs > 0) {
            $properties['DELAY'] = (string) self::delayLevel($message->delayMs);
        }

        return self::encodeProperties($properties);
    }

    /**
     * @param array<string, string> $properties
     */
    public static function encodeProperties(array $properties): string
    {
        $parts = [];

        foreach ($properties as $name => $value) {
            $name = (string) $name;
            $value = (string) $value;

            if ($name === '') {
                throw new \InvalidArgumentException('RocketMQ property names must not be empty.');
            }

            if (strpbrk($name, "\x01\x02") !== false || strpbrk($value, "\x01\x02") !== false) {
                throw new \InvalidArgumentException(
                    'RocketMQ property names and values must not contain the \x01 or \x02 separators.',
                );
            }

            $parts[] = $name . "\x01" . $value;
        }

        return implode("\x02", $parts);
    }

    /**
     * @return array<string, string>
     */
    public static function decodeProperties(string $raw): array
    {
        $properties = [];
        $length = strlen($raw);
        $index = 0;

        while ($index < $length) {
            $end = strpos($raw, "\x02", $index);
            if ($end === false) {
                $end = $length;
            }

            if ($end - $index >= 3) {
                $separator = strpos($raw, "\x01", $index);
                if ($separator !== false && $separator > $index && $separator < $end - 1) {
                    $properties[substr($raw, $index, $separator - $index)] = substr(
                        $raw,
                        $separator + 1,
                        $end - $separator - 1,
                    );
                }
            }

            $index = $end + 1;
        }

        return $properties;
    }

    /**
     * @param array<string, string> $properties
     * @return array<string, string>
     */
    public static function userProperties(array $properties): array
    {
        return array_diff_key($properties, array_flip(self::SYSTEM_PROPERTIES));
    }

    public static function delayLevel(int $delayMs): int
    {
        if ($delayMs <= 0) {
            return 0;
        }

        foreach (self::DELAY_LEVELS_MS as $index => $levelMs) {
            if ($delayMs <= $levelMs) {
                return $index + 1;
            }
        }

        throw new \InvalidArgumentException(sprintf(
            'RocketMQ delay [%d ms] exceeds the maximum delay level of 7200000 ms (2 hours).',
            $delayMs,
        ));
    }

    /**
     * @return list<array{
     *     topic: string,
     *     queueId: int,
     *     flag: int,
     *     queueOffset: int,
     *     physicalOffset: int,
     *     sysFlag: int,
     *     bornTimestamp: int,
     *     storeTimestamp: int,
     *     reconsumeTimes: int,
     *     body: string,
     *     properties: array<string, string>
     * }>
     */
    public static function decodeMessages(string $body): array
    {
        $messages = [];
        $length = strlen($body);
        $offset = 0;

        while ($offset < $length) {
            if ($length - $offset < 4) {
                throw new ProtocolException('Truncated RocketMQ message record (missing total size).');
            }

            $recordStart = $offset;
            $recordSize = self::readUint32($body, $offset);
            if ($recordSize < self::MIN_RECORD_SIZE || $recordSize > $length - $recordStart) {
                throw new ProtocolException(sprintf('Invalid RocketMQ message record size [%d].', $recordSize));
            }

            $recordEnd = $recordStart + $recordSize;

            $magic = self::readUint32($body, $offset);
            if ($magic !== self::MAGIC_CODE && $magic !== self::MAGIC_CODE_V2) {
                throw new ProtocolException(sprintf('Invalid RocketMQ message magic code [%d].', $magic));
            }

            $offset += 4; // body CRC
            $queueId = self::readInt32($body, $offset);
            $flag = self::readInt32($body, $offset);
            $queueOffset = self::readInt64($body, $offset);
            $physicalOffset = self::readInt64($body, $offset);
            $sysFlag = self::readInt32($body, $offset);
            $bornTimestamp = self::readInt64($body, $offset);
            $offset += ($sysFlag & self::BORNHOST_V6_FLAG) === 0 ? 8 : 20;
            $storeTimestamp = self::readInt64($body, $offset);
            $offset += ($sysFlag & self::STOREHOST_V6_FLAG) === 0 ? 8 : 20;
            $reconsumeTimes = self::readInt32($body, $offset);
            $offset += 8; // prepared transaction offset

            $bodyLength = self::readInt32($body, $offset);
            if ($bodyLength < 0 || $offset + $bodyLength > $recordEnd) {
                throw new ProtocolException(sprintf('Invalid RocketMQ message body length [%d].', $bodyLength));
            }
            $messageBody = substr($body, $offset, $bodyLength);
            $offset += $bodyLength;

            if ($offset >= $recordEnd) {
                throw new ProtocolException('Truncated RocketMQ message record (missing topic).');
            }
            $topicLength = ord($body[$offset]);
            $offset += 1;
            if ($offset + $topicLength > $recordEnd) {
                throw new ProtocolException('Truncated RocketMQ message record (invalid topic length).');
            }
            $topic = substr($body, $offset, $topicLength);
            $offset += $topicLength;

            if ($offset + 2 > $recordEnd) {
                throw new ProtocolException('Truncated RocketMQ message record (missing properties length).');
            }
            $propertiesLength = self::readUint16($body, $offset);
            if ($offset + $propertiesLength > $recordEnd) {
                throw new ProtocolException('Truncated RocketMQ message record (invalid properties length).');
            }
            $propertiesRaw = substr($body, $offset, $propertiesLength);
            $offset += $propertiesLength;

            if ($offset !== $recordEnd) {
                throw new ProtocolException(sprintf(
                    'RocketMQ message record size mismatch (parsed %d bytes, declared %d).',
                    $offset - ($recordEnd - $recordSize),
                    $recordSize,
                ));
            }

            if (($sysFlag & self::COMPRESSED_FLAG) === self::COMPRESSED_FLAG) {
                throw new UnsupportedFeatureException('Compressed RocketMQ message bodies are not supported.');
            }

            $messages[] = [
                'topic' => $topic,
                'queueId' => $queueId,
                'flag' => $flag,
                'queueOffset' => $queueOffset,
                'physicalOffset' => $physicalOffset,
                'sysFlag' => $sysFlag,
                'bornTimestamp' => $bornTimestamp,
                'storeTimestamp' => $storeTimestamp,
                'reconsumeTimes' => $reconsumeTimes,
                'body' => $messageBody,
                'properties' => self::decodeProperties($propertiesRaw),
            ];
        }

        return $messages;
    }

    private static function stringifyHeader(string $name, mixed $value): string
    {
        if (is_bool($value)) {
            return $value ? 'true' : 'false';
        }

        if (is_int($value) || is_float($value) || is_string($value)) {
            return (string) $value;
        }

        throw new \InvalidArgumentException(sprintf(
            'RocketMQ header [%s] must be a scalar, %s given.',
            $name,
            get_debug_type($value),
        ));
    }

    private static function readUint32(string $data, int &$offset): int
    {
        self::assertAvailable($data, $offset, 4);
        $value = unpack('N', substr($data, $offset, 4));
        $offset += 4;

        return (int) $value[1];
    }

    private static function readInt32(string $data, int &$offset): int
    {
        $value = self::readUint32($data, $offset);

        return $value >= 0x80000000 ? $value - 0x100000000 : $value;
    }

    private static function readUint16(string $data, int &$offset): int
    {
        self::assertAvailable($data, $offset, 2);
        $value = unpack('n', substr($data, $offset, 2));
        $offset += 2;

        return (int) $value[1];
    }

    private static function readInt64(string $data, int &$offset): int
    {
        self::assertAvailable($data, $offset, 8);
        $value = unpack('J', substr($data, $offset, 8));
        $offset += 8;

        return (int) $value[1];
    }

    private static function assertAvailable(string $data, int $offset, int $length): void
    {
        if ($offset + $length > strlen($data)) {
            throw new ProtocolException('Truncated RocketMQ message record.');
        }
    }
}
