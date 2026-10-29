<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RabbitMq\Amqp;

use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;

/**
 * AMQP 0-9-1 method identifiers plus the builders and parsers the driver and
 * its test broker share.
 */
final class Protocol
{
    public const PROTOCOL_HEADER = "AMQP\x00\x00\x09\x01";

    public const CLASS_CONNECTION = 10;
    public const CLASS_CHANNEL = 20;
    public const CLASS_QUEUE = 50;
    public const CLASS_BASIC = 60;

    public const CONNECTION_START = 10;
    public const CONNECTION_START_OK = 11;
    public const CONNECTION_TUNE = 30;
    public const CONNECTION_TUNE_OK = 31;
    public const CONNECTION_OPEN = 40;
    public const CONNECTION_OPEN_OK = 41;
    public const CONNECTION_CLOSE = 50;
    public const CONNECTION_CLOSE_OK = 51;
    public const CONNECTION_BLOCKED = 60;
    public const CONNECTION_UNBLOCKED = 61;

    public const CHANNEL_OPEN = 10;
    public const CHANNEL_OPEN_OK = 11;
    public const CHANNEL_CLOSE = 40;
    public const CHANNEL_CLOSE_OK = 41;

    public const QUEUE_DECLARE = 10;
    public const QUEUE_DECLARE_OK = 11;

    public const BASIC_QOS = 10;
    public const BASIC_QOS_OK = 11;
    public const BASIC_CONSUME = 20;
    public const BASIC_CONSUME_OK = 21;
    public const BASIC_CANCEL = 30;
    public const BASIC_CANCEL_OK = 31;
    public const BASIC_PUBLISH = 40;
    public const BASIC_RETURN = 50;
    public const BASIC_DELIVER = 60;
    public const BASIC_ACK = 80;
    public const BASIC_REJECT = 90;
    public const BASIC_NACK = 120;

    public static function methodPayload(int $classId, int $methodId, string $args = ''): string
    {
        return Codec::short($classId) . Codec::short($methodId) . $args;
    }

    /**
     * @param array<string, mixed> $properties
     */
    public static function connectionStartOk(
        array $properties,
        string $mechanism,
        string $response,
        string $locale,
    ): string {
        return FieldTable::encode($properties)
            . Codec::shortstr($mechanism)
            . Codec::longstr($response)
            . Codec::shortstr($locale);
    }

    public static function connectionTuneOk(int $channelMax, int $frameMax, int $heartbeat): string
    {
        return Codec::short($channelMax) . Codec::long($frameMax) . Codec::short($heartbeat);
    }

    public static function connectionOpen(string $vhost): string
    {
        return Codec::shortstr($vhost) . Codec::shortstr('') . Codec::bits([false]);
    }

    public static function connectionClose(int $code, string $text): string
    {
        return Codec::short($code) . Codec::shortstr($text) . Codec::short(0) . Codec::short(0);
    }

    public static function channelOpen(): string
    {
        return Codec::shortstr('');
    }

    public static function queueDeclare(string $queue, bool $durable): string
    {
        return Codec::short(0)
            . Codec::shortstr($queue)
            . Codec::bits([false, $durable, false, false, false])
            . FieldTable::encode([]);
    }

    public static function basicQos(int $prefetchCount): string
    {
        return Codec::long(0) . Codec::short($prefetchCount) . Codec::bits([false]);
    }

    public static function basicConsume(string $queue, string $consumerTag): string
    {
        return Codec::short(0)
            . Codec::shortstr($queue)
            . Codec::shortstr($consumerTag)
            . Codec::bits([false, false, false, false])
            . FieldTable::encode([]);
    }

    public static function basicPublish(string $exchange, string $routingKey, bool $mandatory = false): string
    {
        return Codec::short(0)
            . Codec::shortstr($exchange)
            . Codec::shortstr($routingKey)
            . Codec::bits([$mandatory, false]);
    }

    public static function basicAck(int $deliveryTag, bool $multiple = false): string
    {
        return Codec::longlong($deliveryTag) . Codec::bits([$multiple]);
    }

    public static function basicNack(int $deliveryTag, bool $requeue): string
    {
        return Codec::longlong($deliveryTag) . Codec::bits([false, $requeue]);
    }

    /**
     * @return array{class_id: int, method_id: int, args: array<string, mixed>}
     */
    public static function decodeMethod(string $payload): array
    {
        $reader = new Reader($payload);
        $classId = $reader->short();
        $methodId = $reader->short();

        return [
            'class_id' => $classId,
            'method_id' => $methodId,
            'args' => self::decodeArgs($classId, $methodId, $reader),
        ];
    }

    /**
     * @param array<string, mixed> $args
     */
    public static function connectionCloseException(array $args): \Throwable
    {
        $code = (int) ($args['reply_code'] ?? 0);
        $text = (string) ($args['reply_text'] ?? '');
        $message = sprintf('RabbitMQ closed the connection: %s (%d).', $text, $code);

        return $code === 403 ? new AuthenticationException($message) : new ConnectionException($message);
    }

    /**
     * @param array<string, mixed> $args
     */
    public static function channelCloseException(array $args): ProtocolException
    {
        return new ProtocolException(sprintf(
            'RabbitMQ closed the channel: %s (%d).',
            (string) ($args['reply_text'] ?? ''),
            (int) ($args['reply_code'] ?? 0),
        ));
    }

    /**
     * @return array<string, mixed>
     */
    private static function decodeArgs(int $classId, int $methodId, Reader $reader): array
    {
        if ($classId === self::CLASS_CONNECTION) {
            switch ($methodId) {
                case self::CONNECTION_START:
                    return [
                        'version_major' => $reader->octet(),
                        'version_minor' => $reader->octet(),
                        'server_properties' => $reader->table(),
                        'mechanisms' => $reader->longstr(),
                        'locales' => $reader->longstr(),
                    ];
                case self::CONNECTION_START_OK:
                    return [
                        'client_properties' => $reader->table(),
                        'mechanism' => $reader->shortstr(),
                        'response' => $reader->longstr(),
                        'locale' => $reader->shortstr(),
                    ];
                case self::CONNECTION_TUNE:
                case self::CONNECTION_TUNE_OK:
                    return [
                        'channel_max' => $reader->short(),
                        'frame_max' => $reader->long(),
                        'heartbeat' => $reader->short(),
                    ];
                case self::CONNECTION_OPEN:
                    return [
                        'virtual_host' => $reader->shortstr(),
                        'reserved' => $reader->shortstr(),
                        'insist' => $reader->bits(1)[0],
                    ];
                case self::CONNECTION_OPEN_OK:
                    return ['reserved' => $reader->shortstr()];
                case self::CONNECTION_CLOSE:
                    return [
                        'reply_code' => $reader->short(),
                        'reply_text' => $reader->shortstr(),
                        'class_id' => $reader->short(),
                        'method_id' => $reader->short(),
                    ];
                case self::CONNECTION_CLOSE_OK:
                case self::CONNECTION_UNBLOCKED:
                    return [];
                case self::CONNECTION_BLOCKED:
                    return ['reason' => $reader->shortstr()];
            }
        }

        if ($classId === self::CLASS_CHANNEL) {
            switch ($methodId) {
                case self::CHANNEL_OPEN:
                    return ['reserved' => $reader->shortstr()];
                case self::CHANNEL_OPEN_OK:
                    return ['reserved' => $reader->longstr()];
                case self::CHANNEL_CLOSE:
                    return [
                        'reply_code' => $reader->short(),
                        'reply_text' => $reader->shortstr(),
                        'class_id' => $reader->short(),
                        'method_id' => $reader->short(),
                    ];
                case self::CHANNEL_CLOSE_OK:
                    return [];
            }
        }

        if ($classId === self::CLASS_QUEUE) {
            switch ($methodId) {
                case self::QUEUE_DECLARE:
                    $reserved = $reader->short();
                    $queue = $reader->shortstr();
                    $bits = $reader->bits(5);

                    return [
                        'reserved' => $reserved,
                        'queue' => $queue,
                        'passive' => $bits[0],
                        'durable' => $bits[1],
                        'exclusive' => $bits[2],
                        'auto_delete' => $bits[3],
                        'no_wait' => $bits[4],
                        'arguments' => $reader->table(),
                    ];
                case self::QUEUE_DECLARE_OK:
                    return [
                        'queue' => $reader->shortstr(),
                        'message_count' => $reader->long(),
                        'consumer_count' => $reader->long(),
                    ];
            }
        }

        if ($classId === self::CLASS_BASIC) {
            switch ($methodId) {
                case self::BASIC_QOS:
                    return [
                        'prefetch_size' => $reader->long(),
                        'prefetch_count' => $reader->short(),
                        'global' => $reader->bits(1)[0],
                    ];
                case self::BASIC_QOS_OK:
                    return [];
                case self::BASIC_CONSUME:
                    $reserved = $reader->short();
                    $queue = $reader->shortstr();
                    $consumerTag = $reader->shortstr();
                    $bits = $reader->bits(4);

                    return [
                        'reserved' => $reserved,
                        'queue' => $queue,
                        'consumer_tag' => $consumerTag,
                        'no_local' => $bits[0],
                        'no_ack' => $bits[1],
                        'exclusive' => $bits[2],
                        'no_wait' => $bits[3],
                        'arguments' => $reader->table(),
                    ];
                case self::BASIC_CONSUME_OK:
                    return ['consumer_tag' => $reader->shortstr()];
                case self::BASIC_CANCEL:
                    return [
                        'consumer_tag' => $reader->shortstr(),
                        'no_wait' => $reader->bits(1)[0],
                    ];
                case self::BASIC_CANCEL_OK:
                    return ['consumer_tag' => $reader->shortstr()];
                case self::BASIC_PUBLISH:
                    $reserved = $reader->short();
                    $exchange = $reader->shortstr();
                    $routingKey = $reader->shortstr();
                    $bits = $reader->bits(2);

                    return [
                        'reserved' => $reserved,
                        'exchange' => $exchange,
                        'routing_key' => $routingKey,
                        'mandatory' => $bits[0],
                        'immediate' => $bits[1],
                    ];
                case self::BASIC_RETURN:
                    return [
                        'reply_code' => $reader->short(),
                        'reply_text' => $reader->shortstr(),
                        'exchange' => $reader->shortstr(),
                        'routing_key' => $reader->shortstr(),
                    ];
                case self::BASIC_DELIVER:
                    return [
                        'consumer_tag' => $reader->shortstr(),
                        'delivery_tag' => $reader->longlong(),
                        'redelivered' => $reader->bits(1)[0],
                        'exchange' => $reader->shortstr(),
                        'routing_key' => $reader->shortstr(),
                    ];
                case self::BASIC_ACK:
                    return [
                        'delivery_tag' => $reader->longlong(),
                        'multiple' => $reader->bits(1)[0],
                    ];
                case self::BASIC_REJECT:
                    return [
                        'delivery_tag' => $reader->longlong(),
                        'requeue' => $reader->bits(1)[0],
                    ];
                case self::BASIC_NACK:
                    $deliveryTag = $reader->longlong();
                    $bits = $reader->bits(2);

                    return [
                        'delivery_tag' => $deliveryTag,
                        'multiple' => $bits[0],
                        'requeue' => $bits[1],
                    ];
            }
        }

        throw new ProtocolException(sprintf('Unsupported AMQP method %d.%d.', $classId, $methodId));
    }
}
