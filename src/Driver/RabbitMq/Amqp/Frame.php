<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RabbitMq\Amqp;

use Erikwang2013\Messaging\Exception\ProtocolException;

/**
 * AMQP 0-9-1 frame: type (octet), channel (short), payload size (long),
 * payload, frame-end (0xCE).
 */
final class Frame
{
    public const TYPE_METHOD = 1;
    public const TYPE_HEADER = 2;
    public const TYPE_BODY = 3;
    public const TYPE_HEARTBEAT = 8;
    public const END = "\xCE";
    public const HEADER_SIZE = 7;

    public static function encode(int $type, int $channel, string $payload = ''): string
    {
        return chr($type) . Codec::short($channel) . Codec::long(strlen($payload)) . $payload . self::END;
    }

    /**
     * @return array{type: int, channel: int, size: int}
     */
    public static function decodeHeader(string $header): array
    {
        $decoded = unpack('Ctype/nchannel/Nsize', $header);
        if ($decoded === false) {
            throw new ProtocolException('Malformed AMQP frame header.');
        }

        $type = (int) $decoded['type'];
        if (!in_array($type, [self::TYPE_METHOD, self::TYPE_HEADER, self::TYPE_BODY, self::TYPE_HEARTBEAT], true)) {
            throw new ProtocolException(sprintf('Unknown AMQP frame type [%d].', $type));
        }

        return [
            'type' => $type,
            'channel' => (int) $decoded['channel'],
            'size' => (int) $decoded['size'],
        ];
    }

    /**
     * @return array{type: int, channel: int, payload: string}
     */
    public static function decode(string $raw): array
    {
        if (strlen($raw) < self::HEADER_SIZE + 1 || substr($raw, -1) !== self::END) {
            throw new ProtocolException('Malformed AMQP frame: missing frame-end octet.');
        }

        $header = self::decodeHeader(substr($raw, 0, self::HEADER_SIZE));
        if (strlen($raw) !== self::HEADER_SIZE + $header['size'] + 1) {
            throw new ProtocolException('Malformed AMQP frame: payload size does not match the frame length.');
        }

        return [
            'type' => $header['type'],
            'channel' => $header['channel'],
            'payload' => substr($raw, self::HEADER_SIZE, $header['size']),
        ];
    }
}
