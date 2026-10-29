<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Nsq;

use Erikwang2013\Messaging\Exception\ProtocolException;

final class NsqProtocol
{
    public const MAGIC = '  V2';

    public const FRAME_RESPONSE = 0;
    public const FRAME_ERROR = 1;
    public const FRAME_MESSAGE = 2;

    public const HEARTBEAT = '_heartbeat_';

    private const NAME_PATTERN = '/^[.a-zA-Z0-9_-]{1,64}$/';
    private const MESSAGE_HEADER_LENGTH = 26;

    private function __construct()
    {
    }

    public static function publishCommand(string $topic, string $body): string
    {
        self::assertName($topic, 'topic');

        return sprintf("PUB %s\n%s%s", $topic, pack('N', strlen($body)), $body);
    }

    public static function subscribeCommand(string $topic, string $channel): string
    {
        self::assertName($topic, 'topic');
        self::assertName($channel, 'channel');

        return sprintf("SUB %s %s\n", $topic, $channel);
    }

    public static function readyCommand(int $count): string
    {
        if ($count < 1) {
            throw new \InvalidArgumentException('NSQ RDY count must be positive.');
        }

        return sprintf("RDY %d\n", $count);
    }

    public static function finishCommand(string $id): string
    {
        return sprintf("FIN %s\n", $id);
    }

    public static function requeueCommand(string $id, int $delayMs): string
    {
        if ($delayMs < 0) {
            throw new \InvalidArgumentException('NSQ requeue delay must be zero or positive.');
        }

        return sprintf("REQ %s %d\n", $id, $delayMs);
    }

    public static function nopCommand(): string
    {
        return "NOP\n";
    }

    public static function assertName(string $name, string $kind): void
    {
        if (preg_match(self::NAME_PATTERN, $name) !== 1) {
            throw new \InvalidArgumentException(sprintf('Invalid NSQ %s [%s].', $kind, $name));
        }
    }

    /**
     * @return array{timestamp: int, attempts: int, id: string, body: string}
     */
    public static function parseMessage(string $payload): array
    {
        if (strlen($payload) < self::MESSAGE_HEADER_LENGTH) {
            throw new ProtocolException(sprintf(
                'NSQ message payload is too short (%d bytes).',
                strlen($payload),
            ));
        }

        $header = unpack('Jtimestamp/nattempts', substr($payload, 0, 10));
        if ($header === false) {
            throw new ProtocolException('Unable to unpack the NSQ message header.');
        }

        $id = substr($payload, 10, 16);
        if (preg_match('/^[\x20-\x7e]{16}$/', $id) !== 1) {
            throw new ProtocolException('NSQ message id must be 16 ASCII characters.');
        }

        return [
            'timestamp' => (int) $header['timestamp'],
            'attempts' => (int) $header['attempts'],
            'id' => $id,
            'body' => substr($payload, self::MESSAGE_HEADER_LENGTH),
        ];
    }
}
