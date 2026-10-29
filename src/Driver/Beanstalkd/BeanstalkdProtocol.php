<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Beanstalkd;

use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\RequestException;
use Erikwang2013\Messaging\Support\Socket;

final class BeanstalkdProtocol
{
    /**
     * Responses that report a rejected request rather than a malformed exchange.
     */
    private const ERROR_STATUSES = [
        'BURIED',
        'DRAINING',
        'EXPECTED_CRLF',
        'INTERNAL_ERROR',
        'JOB_TOO_BIG',
        'NOT_FOUND',
        'NOT_IGNORED',
        'OUT_OF_MEMORY',
    ];

    /**
     * stream_set_timeout() takes whole seconds; this is far longer than any reserve wait.
     */
    private const BLOCKING_READ_TIMEOUT = 2147483647.0;

    public function __construct(
        private readonly Socket $socket,
        private readonly float $readTimeout = 5.0,
    ) {
    }

    public static function putCommand(int $priority, int $delay, int $ttr, string $body): string
    {
        return sprintf("put %d %d %d %d\r\n%s\r\n", $priority, $delay, $ttr, strlen($body), $body);
    }

    public static function delaySeconds(?int $delayMs): int
    {
        return $delayMs === null ? 0 : (int) ceil($delayMs / 1000);
    }

    /**
     * Beanstalkd reserve timeouts are whole seconds; 0 blocks indefinitely.
     */
    public static function timeoutSeconds(int $timeoutMs): int
    {
        return $timeoutMs === 0 ? 0 : max(1, (int) ceil($timeoutMs / 1000));
    }

    /**
     * @return array{0: string, 1: list<string>}
     */
    public static function parseLine(string $line): array
    {
        $trimmed = rtrim($line, "\r\n");
        if ($trimmed === '') {
            throw new ProtocolException('Beanstalkd returned an empty response line.');
        }

        $separator = strpos($trimmed, ' ');
        if ($separator === false) {
            return [$trimmed, []];
        }

        return [substr($trimmed, 0, $separator), explode(' ', substr($trimmed, $separator + 1))];
    }

    public static function assertStatus(string $expected, string $status, string $command): void
    {
        if ($status === $expected) {
            return;
        }

        $message = sprintf('Beanstalkd command [%s] failed with response [%s].', $command, $status);

        if ($status === 'BAD_FORMAT' || $status === 'UNKNOWN_COMMAND') {
            throw new ProtocolException($message);
        }

        if (in_array($status, self::ERROR_STATUSES, true)) {
            throw new RequestException($message);
        }

        throw new ProtocolException($message);
    }

    /**
     * @return array<string, string>
     */
    public static function parseStats(string $body): array
    {
        $stats = [];
        foreach (explode("\n", $body) as $line) {
            $separator = strpos($line, ':');
            if ($separator === false) {
                continue;
            }

            $stats[trim(substr($line, 0, $separator))] = trim(substr($line, $separator + 1));
        }

        return $stats;
    }

    public function useTube(string $tube): void
    {
        [$status] = $this->command('use ' . $tube);
        self::assertStatus('USING', $status, 'use');
    }

    public function watch(string $tube): void
    {
        [$status] = $this->command('watch ' . $tube);
        self::assertStatus('WATCHING', $status, 'watch');
    }

    public function ignore(string $tube): void
    {
        [$status] = $this->command('ignore ' . $tube);
        self::assertStatus('WATCHING', $status, 'ignore');
    }

    public function put(int $priority, int $delay, int $ttr, string $body): int
    {
        $this->socket->write(self::putCommand($priority, $delay, $ttr, $body));
        [$status, $args] = self::parseLine($this->socket->readLine());
        self::assertStatus('INSERTED', $status, 'put');

        return self::requireInt($args, 0, 'INSERTED id');
    }

    /**
     * @return array{id: int, body: string}|null
     */
    public function reserve(int $timeoutSeconds): ?array
    {
        $readTimeout = $timeoutSeconds === 0
            ? self::BLOCKING_READ_TIMEOUT
            : max($this->readTimeout, $timeoutSeconds + 5.0);

        $this->setReadTimeout($readTimeout);
        try {
            [$status, $args] = $this->command('reserve-with-timeout ' . $timeoutSeconds);
        } finally {
            $this->setReadTimeout($this->readTimeout);
        }

        if ($status === 'TIMED_OUT' || $status === 'DEADLINE_SOON') {
            return null;
        }

        self::assertStatus('RESERVED', $status, 'reserve-with-timeout');
        $id = self::requireInt($args, 0, 'RESERVED id');
        $bytes = self::requireInt($args, 1, 'RESERVED bytes');

        return ['id' => $id, 'body' => $this->readBody($bytes)];
    }

    public function delete(int $id): void
    {
        [$status] = $this->command('delete ' . $id);
        self::assertStatus('DELETED', $status, 'delete');
    }

    public function release(int $id, int $priority, int $delay): void
    {
        [$status] = $this->command(sprintf('release %d %d %d', $id, $priority, $delay));
        self::assertStatus('RELEASED', $status, 'release');
    }

    /**
     * @return array<string, string>
     */
    public function statsJob(int $id): array
    {
        [$status, $args] = $this->command('stats-job ' . $id);
        self::assertStatus('OK', $status, 'stats-job');

        return self::parseStats($this->readBody(self::requireInt($args, 0, 'OK bytes')));
    }

    /**
     * @return array{0: string, 1: list<string>}
     */
    private function command(string $line): array
    {
        $this->socket->write($line . "\r\n");

        return self::parseLine($this->socket->readLine());
    }

    private function readBody(int $bytes): string
    {
        $body = $this->socket->read($bytes);
        if ($this->socket->read(2) !== "\r\n") {
            throw new ProtocolException('Beanstalkd response body was not terminated by CRLF.');
        }

        return $body;
    }

    /**
     * @param list<string> $args
     */
    private static function requireInt(array $args, int $index, string $context): int
    {
        $value = $args[$index] ?? null;
        if ($value === null || preg_match('/\A\d{1,10}\z/', $value) !== 1) {
            throw new ProtocolException(sprintf('Malformed Beanstalkd response: missing %s.', $context));
        }

        return (int) $value;
    }

    private function setReadTimeout(float $seconds): void
    {
        stream_set_timeout($this->socket->stream(), (int) $seconds, (int) (($seconds - floor($seconds)) * 1_000_000));
    }
}
