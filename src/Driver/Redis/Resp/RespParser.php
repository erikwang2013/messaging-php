<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Redis\Resp;

use Erikwang2013\Messaging\Exception\ProtocolException;

/**
 * Parses RESP2 replies from a line reader and a fixed-length byte reader.
 */
final class RespParser
{
    /**
     * @param \Closure(): string $readLine
     * @param \Closure(int): string $readBytes
     */
    public function __construct(
        private \Closure $readLine,
        private \Closure $readBytes,
    ) {
    }

    public function parse(): mixed
    {
        $line = $this->line();
        if ($line === '') {
            throw new ProtocolException('Empty RESP reply.');
        }

        $type = $line[0];
        $payload = substr($line, 1);

        return match ($type) {
            '+' => $payload,
            '-' => throw new ProtocolException($payload),
            ':' => $this->integer($payload),
            '$' => $this->bulkString($payload),
            '*' => $this->array($payload),
            default => throw new ProtocolException(sprintf('Unexpected RESP type byte [%s].', $type)),
        };
    }

    private function line(): string
    {
        $line = ($this->readLine)();
        if (!str_ends_with($line, "\r\n")) {
            throw new ProtocolException('Malformed RESP line: missing CRLF terminator.');
        }

        return substr($line, 0, -2);
    }

    private function integer(string $payload): int
    {
        if (preg_match('/^-?\d+$/', $payload) !== 1) {
            throw new ProtocolException(sprintf('Malformed RESP integer [%s].', $payload));
        }

        return (int) $payload;
    }

    private function bulkString(string $payload): ?string
    {
        $length = $this->integer($payload);
        if ($length === -1) {
            return null;
        }

        if ($length < 0) {
            throw new ProtocolException(sprintf('Malformed RESP bulk length [%d].', $length));
        }

        $value = ($this->readBytes)($length);
        if (strlen($value) !== $length || ($this->readBytes)(2) !== "\r\n") {
            throw new ProtocolException('Malformed RESP bulk string.');
        }

        return $value;
    }

    /**
     * @return list<mixed>|null
     */
    private function array(string $payload): ?array
    {
        $count = $this->integer($payload);
        if ($count === -1) {
            return null;
        }

        if ($count < 0) {
            throw new ProtocolException(sprintf('Malformed RESP array length [%d].', $count));
        }

        $items = [];
        for ($i = 0; $i < $count; $i++) {
            $items[] = $this->parse();
        }

        return $items;
    }
}
