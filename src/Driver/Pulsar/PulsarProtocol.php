<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Pulsar;

use Erikwang2013\Messaging\Driver\Kafka\Crc32c;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;

/**
 * Protobuf command and payload frames for the minimal Pulsar protocol subset.
 *
 * Pulsar shares CRC-32C with Kafka, so the table-driven implementation from the
 * Kafka driver is reused instead of duplicating it.
 */
final class PulsarProtocol
{
    public const MAGIC = 0x0e01;
    public const MAX_FRAME_SIZE = 67_108_864;
    public const PROTOCOL_VERSION = 6;
    public const CLIENT_VERSION = 'messaging-php/1.0.0';

    public const COMMAND_CONNECT = 2;
    public const COMMAND_CONNECTED = 3;
    public const COMMAND_SUBSCRIBE = 4;
    public const COMMAND_PRODUCER = 5;
    public const COMMAND_SEND = 6;
    public const COMMAND_SEND_RECEIPT = 7;
    public const COMMAND_SEND_ERROR = 8;
    public const COMMAND_MESSAGE = 9;
    public const COMMAND_ACK = 10;
    public const COMMAND_FLOW = 11;
    public const COMMAND_SUCCESS = 13;
    public const COMMAND_ERROR = 14;
    public const COMMAND_CLOSE_PRODUCER = 15;
    public const COMMAND_CLOSE_CONSUMER = 16;
    public const COMMAND_PRODUCER_SUCCESS = 17;
    public const COMMAND_PING = 18;
    public const COMMAND_PONG = 19;
    public const COMMAND_REDELIVER = 20;
    public const COMMAND_LOOKUP = 23;
    public const COMMAND_LOOKUP_RESPONSE = 24;
    public const COMMAND_AUTH_CHALLENGE = 34;

    public const LOOKUP_REDIRECT = 0;
    public const LOOKUP_CONNECT = 1;
    public const LOOKUP_FAILED = 2;

    public const SUBSCRIPTION_EXCLUSIVE = 0;
    public const ACK_INDIVIDUAL = 0;
    public const INITIAL_POSITION_EARLIEST = 1;

    /**
     * Wraps a protobuf command in the Pulsar command frame.
     */
    public static function frame(string $command, ?string $payloadFrame = null): string
    {
        if ($command === '') {
            throw new \InvalidArgumentException('A Pulsar command frame cannot be empty.');
        }

        $payload = $payloadFrame ?? '';

        return pack('N', 4 + strlen($command) + strlen($payload))
            . pack('N', strlen($command))
            . $command
            . $payload;
    }

    /**
     * Parses one complete frame from the start of the buffer.
     *
     * @return array{0: PulsarFrame, 1: int}|null frame and consumed byte count, or null when incomplete
     */
    public static function parseFrame(string $buffer): ?array
    {
        if (strlen($buffer) < 4) {
            return null;
        }

        $header = unpack('Nsize', substr($buffer, 0, 4));
        if ($header === false) {
            throw new ProtocolException('Unable to unpack the Pulsar frame size.');
        }

        $totalSize = (int) $header['size'];
        if ($totalSize < 4 || $totalSize > self::MAX_FRAME_SIZE) {
            throw new ProtocolException(sprintf('Invalid Pulsar frame size [%d].', $totalSize));
        }

        if (strlen($buffer) < 4 + $totalSize) {
            return null;
        }

        $commandHeader = unpack('Nsize', substr($buffer, 4, 4));
        if ($commandHeader === false) {
            throw new ProtocolException('Unable to unpack the Pulsar command size.');
        }

        $commandSize = (int) $commandHeader['size'];
        if ($commandSize < 1 || $commandSize > $totalSize - 4) {
            throw new ProtocolException(sprintf('Invalid Pulsar command size [%d] in frame [%d].', $commandSize, $totalSize));
        }

        $commandBytes = substr($buffer, 8, $commandSize);
        $fields = Protobuf::decode($commandBytes);
        $type = $fields[1][0] ?? null;
        if (!is_int($type)) {
            throw new ProtocolException('Pulsar command is missing its type.');
        }

        // The command payload is a nested message stored at the field number
        // equal to the command type; expose its fields directly to callers.
        $body = $fields[$type][0] ?? null;
        $fields = is_string($body) ? Protobuf::decode($body) : [];

        $payloadLength = $totalSize - 4 - $commandSize;
        $payload = $payloadLength > 0 ? substr($buffer, 8 + $commandSize, $payloadLength) : null;

        return [new PulsarFrame($type, $fields, $payload), 4 + $totalSize];
    }

    public static function connectCommand(?string $clientVersion = null): string
    {
        return self::command(
            self::COMMAND_CONNECT,
            Protobuf::stringField(1, $clientVersion ?? self::CLIENT_VERSION)
            . Protobuf::varintField(4, self::PROTOCOL_VERSION),
        );
    }

    public static function lookupCommand(string $topic, int $requestId, bool $authoritative = false): string
    {
        return self::command(
            self::COMMAND_LOOKUP,
            Protobuf::stringField(1, $topic)
            . Protobuf::varintField(2, $requestId)
            . Protobuf::boolField(3, $authoritative),
        );
    }

    public static function producerCommand(
        string $topic,
        int $producerId,
        int $requestId,
        string $producerName,
    ): string {
        return self::command(
            self::COMMAND_PRODUCER,
            Protobuf::stringField(1, $topic)
            . Protobuf::varintField(2, $producerId)
            . Protobuf::varintField(3, $requestId)
            . Protobuf::stringField(4, $producerName),
        );
    }

    public static function sendCommand(int $producerId, int $sequenceId): string
    {
        return self::command(
            self::COMMAND_SEND,
            Protobuf::varintField(1, $producerId)
            . Protobuf::varintField(2, $sequenceId)
            . Protobuf::varintField(3, 1),
        );
    }

    public static function subscribeCommand(
        string $topic,
        string $subscription,
        int $consumerId,
        int $requestId,
        ?string $consumerName = null,
    ): string {
        $body = Protobuf::stringField(1, $topic)
            . Protobuf::stringField(2, $subscription)
            . Protobuf::varintField(3, self::SUBSCRIPTION_EXCLUSIVE)
            . Protobuf::varintField(4, $consumerId)
            . Protobuf::varintField(5, $requestId);

        if ($consumerName !== null) {
            $body .= Protobuf::stringField(6, $consumerName);
        }

        // New subscriptions replay from the beginning so publishing before the
        // first receive() is not lost; Pulsar's default is Latest.
        $body .= Protobuf::varintField(13, self::INITIAL_POSITION_EARLIEST);

        return self::command(self::COMMAND_SUBSCRIBE, $body);
    }

    public static function flowCommand(int $consumerId, int $permits): string
    {
        return self::command(
            self::COMMAND_FLOW,
            Protobuf::varintField(1, $consumerId) . Protobuf::varintField(2, $permits),
        );
    }

    public static function ackCommand(int $consumerId, string $encodedMessageId): string
    {
        return self::command(
            self::COMMAND_ACK,
            Protobuf::varintField(1, $consumerId)
            . Protobuf::varintField(2, self::ACK_INDIVIDUAL)
            . Protobuf::bytesField(3, $encodedMessageId),
        );
    }

    /**
     * An Exclusive subscription can only redeliver all unacknowledged messages.
     */
    public static function redeliverCommand(int $consumerId): string
    {
        return self::command(self::COMMAND_REDELIVER, Protobuf::varintField(1, $consumerId));
    }

    public static function closeProducerCommand(int $producerId, int $requestId): string
    {
        return self::command(
            self::COMMAND_CLOSE_PRODUCER,
            Protobuf::varintField(1, $producerId) . Protobuf::varintField(2, $requestId),
        );
    }

    public static function closeConsumerCommand(int $consumerId, int $requestId): string
    {
        return self::command(
            self::COMMAND_CLOSE_CONSUMER,
            Protobuf::varintField(1, $consumerId) . Protobuf::varintField(2, $requestId),
        );
    }

    public static function pongCommand(): string
    {
        return self::command(self::COMMAND_PONG);
    }

    /**
     * Encodes the magic/CRC/metadata/payload block that follows a Send or Message command.
     *
     * @param array<string, string> $properties
     */
    public static function encodePayload(
        string $producerName,
        int $sequenceId,
        string $payload,
        ?string $key = null,
        array $properties = [],
        ?int $deliverAtMs = null,
    ): string {
        $metadata = Protobuf::stringField(1, $producerName)
            . Protobuf::varintField(2, $sequenceId)
            . Protobuf::varintField(3, self::nowMs());

        foreach ($properties as $name => $value) {
            $metadata .= Protobuf::bytesField(
                4,
                Protobuf::stringField(1, (string) $name) . Protobuf::stringField(2, (string) $value),
            );
        }

        if ($key !== null) {
            $metadata .= Protobuf::stringField(6, $key);
        }

        if ($deliverAtMs !== null) {
            $metadata .= Protobuf::varintField(19, $deliverAtMs);
        }

        $checksumInput = pack('N', strlen($metadata)) . $metadata . $payload;

        return pack('n', self::MAGIC) . pack('N', self::checksum($checksumInput)) . $checksumInput;
    }

    /**
     * @return array{0: array<int, list<int|string>>, 1: string} metadata fields and payload
     */
    public static function decodePayload(string $payloadFrame): array
    {
        if (strlen($payloadFrame) < 10) {
            throw new ProtocolException('Pulsar payload frame is too short.');
        }

        $magic = unpack('nmagic', substr($payloadFrame, 0, 2));
        if ($magic === false || (int) $magic['magic'] !== self::MAGIC) {
            throw new ProtocolException('Invalid Pulsar payload magic.');
        }

        $checksum = unpack('Nchecksum', substr($payloadFrame, 2, 4));
        $metadataSize = unpack('Nsize', substr($payloadFrame, 6, 4));
        if ($checksum === false || $metadataSize === false) {
            throw new ProtocolException('Unable to unpack the Pulsar payload frame header.');
        }

        $size = (int) $metadataSize['size'];
        if ($size < 1 || 10 + $size > strlen($payloadFrame)) {
            throw new ProtocolException(sprintf('Invalid Pulsar metadata size [%d].', $size));
        }

        $checksumInput = substr($payloadFrame, 6);
        if (self::checksum($checksumInput) !== (int) $checksum['checksum']) {
            throw new ProtocolException('Pulsar payload checksum mismatch.');
        }

        $metadata = Protobuf::decode(substr($payloadFrame, 10, $size));

        $compression = Protobuf::intValue($metadata, 8, 0);
        if ($compression !== 0) {
            throw new UnsupportedFeatureException(sprintf(
                'Pulsar compression type [%d] is not supported.',
                $compression,
            ));
        }

        if (Protobuf::intValue($metadata, 11, 0) !== 0) {
            throw new UnsupportedFeatureException('Pulsar batched messages are not supported.');
        }

        return [$metadata, substr($payloadFrame, 10 + $size)];
    }

    /**
     * @param array<string, mixed> $messageId
     */
    public static function messageIdString(array $messageId): string
    {
        $id = sprintf('%d:%d', (int) $messageId['ledger_id'], (int) $messageId['entry_id']);
        if ((int) $messageId['partition'] >= 0) {
            $id .= ':' . (int) $messageId['partition'];
        }

        return $id;
    }

    /**
     * @return array{ledger_id: int, entry_id: int, partition: int, batch_index: int}
     */
    public static function decodeMessageId(string $encoded): array
    {
        $fields = Protobuf::decode($encoded);
        if (!isset($fields[1], $fields[2])) {
            throw new ProtocolException('Pulsar message id is missing its ledger or entry id.');
        }

        return [
            'ledger_id' => Protobuf::intValue($fields, 1),
            'entry_id' => Protobuf::intValue($fields, 2),
            'partition' => Protobuf::intValue($fields, 3, -1),
            'batch_index' => Protobuf::intValue($fields, 4, -1),
        ];
    }

    /**
     * @param array<int, list<int|string>> $metadata
     * @return array<string, string>
     */
    public static function decodeProperties(array $metadata): array
    {
        $properties = [];

        foreach ($metadata[4] ?? [] as $encoded) {
            if (!is_string($encoded)) {
                throw new ProtocolException('Malformed Pulsar message property.');
            }

            $fields = Protobuf::decode($encoded);
            $name = Protobuf::stringValue($fields, 1);
            $value = Protobuf::stringValue($fields, 2);
            if ($name === null || $value === null) {
                throw new ProtocolException('Malformed Pulsar message property.');
            }

            $properties[$name] = $value;
        }

        return $properties;
    }

    /**
     * @param array<int, list<int|string>> $metadata
     */
    public static function decodeKey(array $metadata): ?string
    {
        $key = Protobuf::stringValue($metadata, 6);
        if ($key === null) {
            return null;
        }

        if (Protobuf::intValue($metadata, 17, 0) !== 0) {
            $decoded = base64_decode($key, true);
            if ($decoded !== false) {
                return $decoded;
            }
        }

        return $key;
    }

    public static function checksum(string $data): int
    {
        return Crc32c::compute($data);
    }

    public static function nowMs(): int
    {
        return (int) floor(microtime(true) * 1000);
    }

    private static function command(int $type, string $body = ''): string
    {
        // Every BaseCommand child message is stored at the field number equal
        // to its command type; ping/pong are the only body-less commands.
        return Protobuf::varintField(1, $type) . ($body === '' ? '' : Protobuf::bytesField($type, $body));
    }
}
