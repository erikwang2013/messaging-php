<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Nats;

use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\MessagingException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use JsonException;

/**
 * Pure NATS Core text-protocol encoding and command parsing.
 *
 * NATS Core has no message identifiers or key metadata, so the driver carries
 * them as regular headers when the server supports headers.
 *
 * @internal
 */
final class NatsProtocol
{
    public const VERSION = 'NATS/1.0';
    public const PING = "PING\r\n";
    public const PONG = "PONG\r\n";
    public const ID_HEADER = 'Messaging-Id';
    public const KEY_HEADER = 'Messaging-Key';

    public static function connectPayload(?string $user = null, ?string $pass = null, bool $headers = true): string
    {
        $payload = [
            'verbose' => false,
            'pedantic' => false,
            'tls_required' => false,
            'lang' => 'php',
            'version' => '1.0',
            'protocol' => 1,
            'name' => 'messaging-php',
        ];

        if ($headers) {
            // The server rejects HPUB unless the client declares this.
            $payload['headers'] = true;
        }

        if ($user !== null) {
            $payload['user'] = $user;
            $payload['pass'] = $pass ?? '';
        }

        return 'CONNECT ' . json_encode($payload, JSON_THROW_ON_ERROR) . "\r\n";
    }

    public static function publishFrame(string $subject, string $body, ?string $replyTo = null): string
    {
        return sprintf(
            "PUB %s%s %d\r\n%s\r\n",
            self::subject($subject),
            $replyTo === null ? '' : ' ' . self::subject($replyTo),
            strlen($body),
            $body,
        );
    }

    /**
     * @param array<string, mixed> $headers
     */
    public static function hpubFrame(string $subject, string $body, array $headers, ?string $replyTo = null): string
    {
        $headerBlock = self::encodeHeaderBlock($headers);

        return sprintf(
            "HPUB %s%s %d %d\r\n%s%s\r\n",
            self::subject($subject),
            $replyTo === null ? '' : ' ' . self::subject($replyTo),
            strlen($headerBlock),
            strlen($headerBlock) + strlen($body),
            $headerBlock,
            $body,
        );
    }

    public static function subscribeFrame(string $subject, ?string $queueGroup, string $sid): string
    {
        self::subject($subject, true);

        if ($queueGroup !== null) {
            if ($queueGroup === '' || preg_match('/\s/', $queueGroup) === 1) {
                throw new ProtocolException('NATS queue group must be a non-empty token without whitespace.');
            }
            $queueGroup = ' ' . $queueGroup;
        }

        return sprintf("SUB %s%s %s\r\n", $subject, $queueGroup ?? '', $sid);
    }

    /**
     * @return array{
     *     type: string,
     *     subject?: string,
     *     sid?: string,
     *     reply?: string|null,
     *     size?: int,
     *     headerSize?: int,
     *     totalSize?: int,
     *     message?: string,
     *     data?: array<string, mixed>
     * }
     */
    public static function parseCommand(string $line): array
    {
        $line = rtrim($line, "\r\n");

        if ($line === 'PING') {
            return ['type' => 'PING'];
        }
        if ($line === 'PONG') {
            return ['type' => 'PONG'];
        }
        if ($line === '+OK') {
            return ['type' => 'OK'];
        }

        if (str_starts_with($line, 'INFO ')) {
            try {
                $data = json_decode(substr($line, 5), true, 512, JSON_THROW_ON_ERROR);
            } catch (JsonException $exception) {
                throw new ProtocolException('Malformed INFO payload from NATS server.', 0, $exception);
            }
            if (!is_array($data)) {
                throw new ProtocolException('Malformed INFO payload from NATS server.');
            }

            /** @var array<string, mixed> $data */
            return ['type' => 'INFO', 'data' => $data];
        }

        if (str_starts_with($line, '-ERR')) {
            return ['type' => 'ERR', 'message' => trim(trim(substr($line, 4)), "'\"")];
        }

        $tokens = preg_split('/ +/', $line) ?: [];
        $command = $tokens[0] ?? '';

        if ($command === 'MSG') {
            return self::parseMessageCommand($tokens, false);
        }
        if ($command === 'HMSG') {
            return self::parseMessageCommand($tokens, true);
        }

        throw new ProtocolException(sprintf('Unexpected NATS protocol line [%s].', substr($line, 0, 64)));
    }

    /**
     * @return array{status: int|null, headers: array<string, mixed>}
     */
    public static function parseHeaderBlock(string $block): array
    {
        $lines = explode("\r\n", $block);
        $first = array_shift($lines);
        if ($first === null || !str_starts_with($first, self::VERSION)) {
            throw new ProtocolException('Malformed NATS header block.');
        }

        $status = null;
        $statusText = trim(substr($first, strlen(self::VERSION)));
        if ($statusText !== '') {
            $statusToken = explode(' ', $statusText)[0];
            if (ctype_digit($statusToken)) {
                $status = (int) $statusToken;
            }
        }

        $headers = [];
        foreach ($lines as $line) {
            if ($line === '') {
                break;
            }

            $separator = strpos($line, ':');
            if ($separator === false || $separator === 0) {
                throw new ProtocolException(sprintf('Malformed NATS header line [%s].', substr($line, 0, 64)));
            }

            $headers[substr($line, 0, $separator)] = self::decodeHeaderValue(ltrim(substr($line, $separator + 1), ' '));
        }

        return ['status' => $status, 'headers' => $headers];
    }

    /**
     * @param array<string, mixed> $headers
     */
    public static function encodeHeaderBlock(array $headers): string
    {
        $block = self::VERSION . "\r\n";
        foreach ($headers as $name => $value) {
            $name = (string) $name;
            if ($name === '' || preg_match('/[\s:]/', $name) === 1) {
                throw new ProtocolException(sprintf('Invalid NATS header name [%s].', substr($name, 0, 64)));
            }

            $value = self::encodeHeaderValue($value);
            if (preg_match('/[\r\n]/', $value) === 1) {
                throw new ProtocolException(sprintf('NATS header [%s] contains a line break.', $name));
            }

            $block .= $name . ': ' . $value . "\r\n";
        }

        return $block . "\r\n";
    }

    public static function errorException(string $message): MessagingException
    {
        $lower = strtolower($message);
        if (str_contains($lower, 'authorization') || str_contains($lower, 'authentication')) {
            return new AuthenticationException($message);
        }

        return new ProtocolException($message === '' ? 'NATS server reported an error.' : $message);
    }

    /**
     * @param list<string> $tokens
     * @return array<string, mixed>
     */
    private static function parseMessageCommand(array $tokens, bool $withHeaders): array
    {
        $expected = $withHeaders ? [5, 6] : [4, 5];
        if (!in_array(count($tokens), $expected, true)) {
            throw new ProtocolException(sprintf(
                'Malformed %s line, expected %d or %d tokens.',
                $tokens[0] ?? 'MSG',
                $expected[0],
                $expected[1],
            ));
        }

        $subject = $tokens[1];
        $sid = $tokens[2];
        if ($subject === '' || $sid === '') {
            throw new ProtocolException('Malformed message line, subject and subscription id are required.');
        }

        $arguments = array_slice($tokens, 3);
        $reply = null;
        if (count($arguments) === ($withHeaders ? 3 : 2)) {
            $reply = array_shift($arguments);
            if ($reply === '') {
                throw new ProtocolException('Malformed message line, empty reply subject.');
            }
        }

        if (!$withHeaders) {
            return [
                'type' => 'MSG',
                'subject' => $subject,
                'sid' => $sid,
                'reply' => $reply,
                'size' => self::nonNegativeInt($arguments[0]),
            ];
        }

        $headerSize = self::nonNegativeInt($arguments[0]);
        $totalSize = self::nonNegativeInt($arguments[1]);
        if ($headerSize > $totalSize) {
            throw new ProtocolException('Malformed HMSG line, header size exceeds total size.');
        }

        return [
            'type' => 'HMSG',
            'subject' => $subject,
            'sid' => $sid,
            'reply' => $reply,
            'headerSize' => $headerSize,
            'totalSize' => $totalSize,
        ];
    }

    private static function nonNegativeInt(string $value): int
    {
        if ($value === '' || !ctype_digit($value)) {
            throw new ProtocolException(sprintf('Expected a non-negative integer in NATS frame, got [%s].', $value));
        }

        return (int) $value;
    }

    private static function subject(string $subject, bool $allowWildcards = false): string
    {
        if ($subject === '' || preg_match('/\s/', $subject) === 1) {
            throw new ProtocolException('NATS subject must be a non-empty token without whitespace.');
        }
        if (!$allowWildcards && (str_contains($subject, '*') || str_contains($subject, '>'))) {
            throw new ProtocolException('NATS subjects cannot contain wildcards when publishing.');
        }

        return $subject;
    }

    private static function encodeHeaderValue(mixed $value): string
    {
        if (is_string($value)) {
            return $value;
        }
        if ($value === null) {
            return '';
        }
        if (is_bool($value)) {
            return $value ? 'true' : 'false';
        }
        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }

        return (string) json_encode($value, JSON_THROW_ON_ERROR);
    }

    private static function decodeHeaderValue(string $value): mixed
    {
        // NATS headers are strings on the wire; do not guess types here or a
        // string header like "00123" silently becomes an integer.
        return $value;
    }
}
