<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RocketMq;

use Erikwang2013\Messaging\Exception\ProtocolException;

/**
 * RocketMQ 4.x remoting constants and frame codec (JSON header serialization).
 */
final class RocketMqProtocol
{
    /** Version field sent by the 4.9.x Java client (MQVersion.V4_9_7.ordinal()). */
    public const VERSION = 407;

    public const SEND_MESSAGE = 10;
    public const PULL_MESSAGE = 11;
    public const QUERY_CONSUMER_OFFSET = 14;
    public const UPDATE_CONSUMER_OFFSET = 15;
    public const GET_MAX_OFFSET = 30;
    public const GET_MIN_OFFSET = 31;
    public const HEART_BEAT = 34;
    public const CONSUMER_SEND_MSG_BACK = 36;
    public const GET_ROUTEINFO_BY_TOPIC = 105;

    public const SUCCESS = 0;
    public const SYSTEM_ERROR = 1;
    public const SYSTEM_BUSY = 2;
    public const SERVICE_NOT_AVAILABLE = 14;
    public const NO_PERMISSION = 16;
    public const TOPIC_NOT_EXIST = 17;
    public const PULL_NOT_FOUND = 19;
    public const PULL_RETRY_IMMEDIATELY = 20;
    public const PULL_OFFSET_MOVED = 21;
    public const QUERY_NOT_FOUND = 22;
    public const FLOW_CONTROL = 215;

    public const DEFAULT_TOPIC = 'TBW102';
    public const RETRY_TOPIC_PREFIX = '%RETRY%';

    public const PERM_READ = 0x4;
    public const PERM_WRITE = 0x2;

    /** Upper bound on a single remoting frame, mirroring the broker's default. */
    public const MAX_FRAME_SIZE = 67108864;

    private function __construct()
    {
    }

    public static function retryTopic(string $group): string
    {
        return self::RETRY_TOPIC_PREFIX . $group;
    }

    /**
     * @param array<string, string> $extFields
     */
    public static function encodeRequest(int $code, int $opaque, array $extFields, string $body): string
    {
        $header = [
            'code' => $code,
            'language' => 'PHP',
            'version' => self::VERSION,
            'opaque' => $opaque,
            'flag' => 0,
        ];

        if ($extFields !== []) {
            $header['extFields'] = $extFields;
        }

        return self::encode($header, $body);
    }

    /**
     * @param array<string, mixed> $header
     */
    private static function encode(array $header, string $body): string
    {
        try {
            $json = json_encode($header, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        } catch (\JsonException $exception) {
            throw new ProtocolException('Unable to encode the RocketMQ command header: ' . $exception->getMessage(), 0, $exception);
        }

        $headerLength = strlen($json);
        if ($headerLength > 0xFFFFFF) {
            throw new ProtocolException('RocketMQ command header is too large.');
        }

        // totalLen covers the 4-byte header-length field, the header and the body.
        return pack('NN', 4 + $headerLength + strlen($body), $headerLength) . $json . $body;
    }

    public static function decodeFrame(string $frame): RocketMqCommand
    {
        $frameLength = strlen($frame);
        if ($frameLength < 8) {
            throw new ProtocolException(sprintf('RocketMQ frame is too short (%d bytes).', $frameLength));
        }

        $length = self::unpackUint32(substr($frame, 0, 4));
        if ($length !== $frameLength - 4) {
            throw new ProtocolException(sprintf(
                'RocketMQ frame length mismatch (declared %d, received %d).',
                $length,
                $frameLength - 4,
            ));
        }
        if ($length > self::MAX_FRAME_SIZE) {
            throw new ProtocolException(sprintf('RocketMQ frame is too large (%d bytes).', $length));
        }

        $headerField = self::unpackUint32(substr($frame, 4, 4));
        $serializeType = ($headerField >> 24) & 0xFF;
        if ($serializeType !== 0) {
            throw new ProtocolException(sprintf('Unsupported RocketMQ header serialize type [%d].', $serializeType));
        }

        $headerLength = $headerField & 0xFFFFFF;
        if ($headerLength > $length - 4) {
            throw new ProtocolException(sprintf(
                'RocketMQ header length [%d] exceeds the frame payload [%d].',
                $headerLength,
                $length - 4,
            ));
        }

        try {
            $header = json_decode(substr($frame, 8, $headerLength), true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $exception) {
            throw new ProtocolException('Invalid RocketMQ command header JSON: ' . $exception->getMessage(), 0, $exception);
        }

        if (!is_array($header)) {
            throw new ProtocolException('RocketMQ command header must be a JSON object.');
        }

        $code = $header['code'] ?? null;
        $opaque = $header['opaque'] ?? null;
        if (!is_int($code) || !is_int($opaque)) {
            throw new ProtocolException('RocketMQ command header is missing integer [code] or [opaque].');
        }

        $extFields = [];
        if (isset($header['extFields'])) {
            if (!is_array($header['extFields'])) {
                throw new ProtocolException('RocketMQ command header field [extFields] must be an object.');
            }

            foreach ($header['extFields'] as $name => $value) {
                if (!is_scalar($value)) {
                    throw new ProtocolException(sprintf('RocketMQ extField [%s] must be a scalar.', (string) $name));
                }

                $extFields[(string) $name] = (string) $value;
            }
        }

        $remark = $header['remark'] ?? null;

        return new RocketMqCommand(
            $code,
            $opaque,
            is_int($header['flag'] ?? null) ? $header['flag'] : 0,
            is_string($remark) ? $remark : null,
            $extFields,
            substr($frame, 8 + $headerLength),
        );
    }

    private static function unpackUint32(string $bytes): int
    {
        $value = unpack('N', $bytes);
        if ($value === false) {
            throw new ProtocolException('Unable to unpack a RocketMQ frame field.');
        }

        return (int) $value[1];
    }
}
