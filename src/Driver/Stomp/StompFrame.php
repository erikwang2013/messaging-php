<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Stomp;

use Erikwang2013\Messaging\Exception\ProtocolException;

/**
 * A STOMP 1.2 frame. Headers are stored exactly as they appear on the wire
 * (after unescaping); lookups are case-insensitive.
 */
final class StompFrame
{
    /**
     * CONNECT, STOMP and CONNECTED headers are never escaped (STOMP 1.2, 3.1.2).
     */
    private const UNESCAPED_COMMANDS = ['CONNECT', 'STOMP', 'CONNECTED'];

    /**
     * @param array<string, string> $headers
     */
    public function __construct(
        public string $command,
        public array $headers = [],
        public string $body = '',
    ) {
    }

    public function header(string $name): ?string
    {
        foreach ($this->headers as $key => $value) {
            if (strcasecmp($key, $name) === 0) {
                return $value;
            }
        }

        return null;
    }

    public static function encode(self $frame): string
    {
        if ($frame->command === '' || preg_match('/[\r\n\x00]/', $frame->command) === 1) {
            throw new ProtocolException('STOMP commands must be non-empty single-line strings.');
        }

        $headers = $frame->headers;
        if ($frame->body !== '' && self::headerValue($headers, 'content-length') === null) {
            $headers['content-length'] = (string) strlen($frame->body);
        }

        $escape = !in_array($frame->command, self::UNESCAPED_COMMANDS, true);
        $lines = [$frame->command];
        foreach ($headers as $name => $value) {
            if ($name === '' || preg_match('/[\r\n:]/', $name) === 1) {
                throw new ProtocolException(sprintf('Invalid STOMP header name [%s].', $name));
            }

            $lines[] = $name . ':' . ($escape ? self::escapeHeader($value) : $value);
        }

        return implode("\n", $lines) . "\n\n" . $frame->body . "\x00";
    }

    public static function decode(string $raw): self
    {
        if (!str_ends_with($raw, "\x00")) {
            throw new ProtocolException('STOMP frame is not NUL terminated.');
        }

        [$headerBlock, $body] = self::split(substr($raw, 0, -1));

        return self::parse($headerBlock, $body);
    }

    public static function parse(string $headerBlock, string $body): self
    {
        [$command, $headers] = self::parseHeaderBlock($headerBlock);
        $contentLength = self::headerValue($headers, 'content-length');

        if ($contentLength !== null) {
            if (preg_match('/^\d+$/', $contentLength) !== 1) {
                throw new ProtocolException('STOMP content-length header must be a non-negative integer.');
            }

            if ((int) $contentLength !== strlen($body)) {
                throw new ProtocolException(sprintf(
                    'STOMP content-length [%s] does not match the body length [%d].',
                    $contentLength,
                    strlen($body),
                ));
            }
        }

        return new self($command, $headers, $body);
    }

    /**
     * Returns the declared content-length of a raw header block, or null when absent.
     */
    public static function contentLength(string $headerBlock): ?int
    {
        [, $headers] = self::parseHeaderBlock($headerBlock);
        $value = self::headerValue($headers, 'content-length');
        if ($value === null) {
            return null;
        }

        if (preg_match('/^\d+$/', $value) !== 1) {
            throw new ProtocolException('STOMP content-length header must be a non-negative integer.');
        }

        return (int) $value;
    }

    /**
     * @param array<string, string> $headers
     */
    private static function headerValue(array $headers, string $name): ?string
    {
        foreach ($headers as $key => $value) {
            if (strcasecmp($key, $name) === 0) {
                return $value;
            }
        }

        return null;
    }

    /**
     * @return array{string, string}
     */
    private static function split(string $raw): array
    {
        $lf = strpos($raw, "\n\n");
        $crlf = strpos($raw, "\r\n\r\n");

        if ($lf !== false && ($crlf === false || $lf <= $crlf)) {
            return [substr($raw, 0, $lf), substr($raw, $lf + 2)];
        }

        if ($crlf !== false) {
            return [substr($raw, 0, $crlf), substr($raw, $crlf + 4)];
        }

        throw new ProtocolException('STOMP frame is missing the header/body separator.');
    }

    /**
     * @return array{string, array<string, string>}
     */
    private static function parseHeaderBlock(string $headerBlock): array
    {
        $lines = explode("\n", $headerBlock);
        $command = rtrim(array_shift($lines) ?? '', "\r");

        if ($command === '' || preg_match('/[\r\x00]/', $command) === 1) {
            throw new ProtocolException('STOMP frame is missing a valid command.');
        }

        $escape = !in_array($command, self::UNESCAPED_COMMANDS, true);
        $headers = [];

        foreach ($lines as $line) {
            $line = rtrim($line, "\r");
            if ($line === '') {
                throw new ProtocolException('STOMP frame contains a blank line inside the header block.');
            }

            $separator = strpos($line, ':');
            if ($separator === false || $separator === 0) {
                throw new ProtocolException(sprintf('Malformed STOMP header line [%s].', $line));
            }

            $name = substr($line, 0, $separator);
            $value = substr($line, $separator + 1);
            if ($escape) {
                $value = self::unescapeHeader($value);
            }

            // STOMP 1.2: the first occurrence of a repeated header wins.
            if (self::headerValue($headers, $name) === null) {
                $headers[$name] = $value;
            }
        }

        return [$command, $headers];
    }

    private static function escapeHeader(string $value): string
    {
        return strtr($value, [
            '\\' => '\\\\',
            "\r" => '\\r',
            "\n" => '\\n',
            ':' => '\\c',
        ]);
    }

    private static function unescapeHeader(string $value): string
    {
        if (!str_contains($value, '\\')) {
            return $value;
        }

        $result = '';
        $length = strlen($value);

        for ($i = 0; $i < $length; $i++) {
            if ($value[$i] !== '\\') {
                $result .= $value[$i];
                continue;
            }

            $i++;
            if ($i >= $length) {
                throw new ProtocolException('STOMP header ends with a dangling escape character.');
            }

            $result .= match ($value[$i]) {
                '\\' => '\\',
                'n' => "\n",
                'r' => "\r",
                'c' => ':',
                default => throw new ProtocolException(sprintf('Invalid STOMP header escape [\\%s].', $value[$i])),
            };
        }

        return $result;
    }
}
