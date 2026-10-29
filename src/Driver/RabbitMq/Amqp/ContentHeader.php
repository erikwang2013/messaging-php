<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RabbitMq\Amqp;

use Erikwang2013\Messaging\Exception\ProtocolException;

/**
 * basic class (60) content header. Only the properties the driver needs are
 * encoded; all others are decoded and returned in the properties array.
 */
final class ContentHeader
{
    private const CLASS_BASIC = 60;

    private const FLAG_CONTENT_TYPE = 0x8000;
    private const FLAG_CONTENT_ENCODING = 0x4000;
    private const FLAG_HEADERS = 0x2000;
    private const FLAG_DELIVERY_MODE = 0x1000;
    private const FLAG_PRIORITY = 0x0800;
    private const FLAG_CORRELATION_ID = 0x0400;
    private const FLAG_REPLY_TO = 0x0200;
    private const FLAG_EXPIRATION = 0x0100;
    private const FLAG_MESSAGE_ID = 0x0080;
    private const FLAG_TIMESTAMP = 0x0040;
    private const FLAG_TYPE = 0x0020;
    private const FLAG_USER_ID = 0x0010;
    private const FLAG_APP_ID = 0x0008;
    private const FLAG_CLUSTER_ID = 0x0004;
    private const FLAG_MORE = 0x0001;

    /**
     * @param array<string, mixed> $headers
     */
    public static function encode(
        int $bodySize,
        ?string $messageId,
        ?string $correlationId,
        array $headers,
        int $deliveryMode,
    ): string {
        $flags = 0;
        $properties = '';

        if ($headers !== []) {
            $flags |= self::FLAG_HEADERS;
            $properties .= FieldTable::encode($headers);
        }

        if ($deliveryMode > 0) {
            $flags |= self::FLAG_DELIVERY_MODE;
            $properties .= chr($deliveryMode);
        }

        if ($correlationId !== null) {
            $flags |= self::FLAG_CORRELATION_ID;
            $properties .= Codec::shortstr($correlationId);
        }

        if ($messageId !== null) {
            $flags |= self::FLAG_MESSAGE_ID;
            $properties .= Codec::shortstr($messageId);
        }

        return Codec::short(self::CLASS_BASIC)
            . Codec::short(0)
            . Codec::longlong($bodySize)
            . Codec::short($flags)
            . $properties;
    }

    /**
     * @return array{class_id: int, body_size: int, properties: array<string, mixed>}
     */
    public static function decode(string $payload): array
    {
        $reader = new Reader($payload);
        $classId = $reader->short();
        if ($classId !== self::CLASS_BASIC) {
            throw new ProtocolException(sprintf('Unexpected content header class [%d], expected basic.', $classId));
        }

        $reader->short();
        $bodySize = $reader->longlong();
        $flags = $reader->short();

        if (($flags & self::FLAG_MORE) !== 0) {
            throw new ProtocolException('Continuation AMQP content header property flags are not supported.');
        }

        $properties = [];

        if (($flags & self::FLAG_CONTENT_TYPE) !== 0) {
            $properties['content_type'] = $reader->shortstr();
        }
        if (($flags & self::FLAG_CONTENT_ENCODING) !== 0) {
            $properties['content_encoding'] = $reader->shortstr();
        }
        if (($flags & self::FLAG_HEADERS) !== 0) {
            $properties['headers'] = $reader->table();
        }
        if (($flags & self::FLAG_DELIVERY_MODE) !== 0) {
            $properties['delivery_mode'] = $reader->octet();
        }
        if (($flags & self::FLAG_PRIORITY) !== 0) {
            $properties['priority'] = $reader->octet();
        }
        if (($flags & self::FLAG_CORRELATION_ID) !== 0) {
            $properties['correlation_id'] = $reader->shortstr();
        }
        if (($flags & self::FLAG_REPLY_TO) !== 0) {
            $properties['reply_to'] = $reader->shortstr();
        }
        if (($flags & self::FLAG_EXPIRATION) !== 0) {
            $properties['expiration'] = $reader->shortstr();
        }
        if (($flags & self::FLAG_MESSAGE_ID) !== 0) {
            $properties['message_id'] = $reader->shortstr();
        }
        if (($flags & self::FLAG_TIMESTAMP) !== 0) {
            $properties['timestamp'] = $reader->longlong();
        }
        if (($flags & self::FLAG_TYPE) !== 0) {
            $properties['type'] = $reader->shortstr();
        }
        if (($flags & self::FLAG_USER_ID) !== 0) {
            $properties['user_id'] = $reader->shortstr();
        }
        if (($flags & self::FLAG_APP_ID) !== 0) {
            $properties['app_id'] = $reader->shortstr();
        }
        if (($flags & self::FLAG_CLUSTER_ID) !== 0) {
            $properties['cluster_id'] = $reader->shortstr();
        }

        return ['class_id' => $classId, 'body_size' => $bodySize, 'properties' => $properties];
    }
}
