<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Redis;

use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;

/**
 * Builds Redis Streams commands as RESP argument lists.
 *
 * @internal
 */
final class StreamsCommand
{
    /**
     * @return list<string|int>
     */
    public static function createGroup(string $stream, string $group): array
    {
        // Start at 0 so a queue consumer sees messages published before the
        // group existed; starting at $ would silently skip that backlog.
        return ['XGROUP', 'CREATE', $stream, $group, '0', 'MKSTREAM'];
    }

    /**
     * @param array<string, mixed> $headers
     * @return list<string|int>
     */
    public static function add(
        string $stream,
        string $body,
        array $headers,
        ?string $key,
        ?int $maxLength,
    ): array {
        $command = ['XADD', $stream];

        if ($maxLength !== null) {
            $command[] = 'MAXLEN';
            $command[] = '~';
            $command[] = $maxLength;
        }

        $command[] = '*';
        $command[] = 'body';
        $command[] = $body;

        if ($headers !== []) {
            try {
                $encoded = json_encode($headers, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);
            } catch (\JsonException $exception) {
                throw new UnsupportedFeatureException(
                    'Redis Streams headers must be JSON-serializable.',
                    0,
                    $exception,
                );
            }

            $command[] = 'headers';
            $command[] = $encoded;
        }

        if ($key !== null) {
            $command[] = 'key';
            $command[] = $key;
        }

        return $command;
    }

    /**
     * @return list<string|int>
     */
    public static function readGroup(
        string $stream,
        string $group,
        string $consumer,
        int $timeoutMs,
    ): array {
        return ['XREADGROUP', 'GROUP', $group, $consumer, 'COUNT', 1, 'BLOCK', $timeoutMs, 'STREAMS', $stream, '>'];
    }

    /**
     * @return list<string|int>
     */
    public static function ack(string $stream, string $group, string $id): array
    {
        return ['XACK', $stream, $group, $id];
    }
}
