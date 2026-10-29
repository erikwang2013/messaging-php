<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\ZeroMq;

use Erikwang2013\Messaging\Exception\ProtocolException;

/**
 * ZMTP 3.1 wire encoding for the NULL mechanism.
 *
 * Wire format (RFC 23/ZMTP):
 *   greeting = %xFF + 8 bytes padding + %x7F + version + mechanism + as-server + filler (64 bytes)
 *   frame    = flags + size (1 or 8 bytes) + body; flags: MORE 0x01, LONG 0x02, COMMAND 0x04
 *
 * @internal
 */
final class ZmtpProtocol
{
    public const GREETING_SIZE = 64;
    public const FLAG_MORE = 0x01;
    public const FLAG_LONG = 0x02;
    public const FLAG_COMMAND = 0x04;
    public const VERSION_MAJOR = 3;
    public const VERSION_MINOR = 1;
    public const MECHANISM = 'NULL';

    /** @var array<string, string> local socket type => compatible peer socket type */
    private const COMPATIBLE_PEERS = [
        'PUB' => 'SUB',
        'SUB' => 'PUB',
        'PUSH' => 'PULL',
        'PULL' => 'PUSH',
    ];

    public static function greeting(): string
    {
        return "\xFF" . str_repeat("\x00", 8) . "\x7F"
            . chr(self::VERSION_MAJOR) . chr(self::VERSION_MINOR)
            . str_pad(self::MECHANISM, 20, "\x00")
            . "\x00"
            . str_repeat("\x00", 31);
    }

    public static function validateGreeting(string $greeting): void
    {
        if (strlen($greeting) !== self::GREETING_SIZE) {
            throw new ProtocolException(sprintf(
                'Malformed ZMTP greeting: expected %d bytes, got %d.',
                self::GREETING_SIZE,
                strlen($greeting),
            ));
        }

        if ($greeting[0] !== "\xFF" || $greeting[9] !== "\x7F") {
            throw new ProtocolException('Malformed ZMTP greeting: invalid signature.');
        }

        $major = ord($greeting[10]);
        $minor = ord($greeting[11]);
        if ($major !== self::VERSION_MAJOR) {
            throw new ProtocolException(sprintf(
                'Unsupported ZMTP version %d.%d; only 3.x is supported.',
                $major,
                $minor,
            ));
        }

        $mechanism = rtrim(substr($greeting, 12, 20), "\x00");
        if ($mechanism !== self::MECHANISM) {
            throw new ProtocolException(sprintf(
                'Unsupported ZMTP mechanism [%s]; only NULL is supported.',
                $mechanism,
            ));
        }

        if ($greeting[32] !== "\x00") {
            throw new ProtocolException('Malformed ZMTP greeting: the NULL mechanism must not act as server.');
        }
    }

    public static function readyFrame(string $socketType, ?string $identity = null): string
    {
        $metadata = self::property('Socket-Type', $socketType);
        if ($identity !== null) {
            $metadata .= self::property('Identity', $identity);
        }
        $metadata .= "\x00";

        return self::frame(self::FLAG_COMMAND, chr(strlen('READY')) . 'READY' . $metadata);
    }

    /**
     * Subscription is a regular message frame: 0x01 (subscribe) followed by the topic prefix.
     */
    public static function subscribeBody(string $topic): string
    {
        return "\x01" . $topic;
    }

    public static function frame(int $flags, string $body): string
    {
        $length = strlen($body);
        if ($length > 0xFF) {
            return chr($flags | self::FLAG_LONG) . pack('J', $length) . $body;
        }

        return chr($flags) . chr($length) . $body;
    }

    /**
     * @param list<string> $frames
     */
    public static function message(array $frames): string
    {
        $last = count($frames) - 1;
        $data = '';
        foreach ($frames as $index => $frame) {
            $data .= self::frame($index < $last ? self::FLAG_MORE : 0, $frame);
        }

        return $data;
    }

    /**
     * @return array{name: string, metadata: array<string, string>}
     */
    public static function parseCommand(string $body): array
    {
        if ($body === '') {
            throw new ProtocolException('Malformed ZMTP command: empty body.');
        }

        $nameLength = ord($body[0]);
        if ($nameLength === 0 || strlen($body) < 1 + $nameLength) {
            throw new ProtocolException('Malformed ZMTP command: invalid command name.');
        }

        return [
            'name' => substr($body, 1, $nameLength),
            'metadata' => self::parseMetadata(substr($body, 1 + $nameLength)),
        ];
    }

    /**
     * Metadata properties: 1-byte name length, name, 4-byte big-endian value length, value;
     * the list ends with a zero name length.
     *
     * @return array<string, string>
     */
    public static function parseMetadata(string $data): array
    {
        $properties = [];
        $offset = 0;
        $length = strlen($data);

        while ($offset < $length) {
            $nameLength = ord($data[$offset]);
            $offset++;
            if ($nameLength === 0) {
                return $properties;
            }

            if ($offset + $nameLength + 4 > $length) {
                throw new ProtocolException('Malformed ZMTP metadata property name.');
            }
            $name = substr($data, $offset, $nameLength);
            $offset += $nameLength;

            $unpacked = unpack('N', substr($data, $offset, 4));
            $valueLength = is_array($unpacked) ? (int) $unpacked[1] : 0;
            $offset += 4;
            if ($offset + $valueLength > $length) {
                throw new ProtocolException(sprintf('Malformed ZMTP metadata value for property [%s].', $name));
            }

            $properties[$name] = substr($data, $offset, $valueLength);
            $offset += $valueLength;
        }

        return $properties;
    }

    public static function compatiblePeerType(string $localSocketType): string
    {
        if (!isset(self::COMPATIBLE_PEERS[$localSocketType])) {
            throw new \InvalidArgumentException(sprintf(
                'Unsupported ZeroMQ socket type [%s].',
                $localSocketType,
            ));
        }

        return self::COMPATIBLE_PEERS[$localSocketType];
    }

    private static function property(string $name, string $value): string
    {
        return chr(strlen($name)) . $name . pack('N', strlen($value)) . $value;
    }
}
