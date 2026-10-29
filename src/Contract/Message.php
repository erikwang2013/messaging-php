<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Contract;

final class Message
{
    /**
     * @param array<string, mixed> $headers
     */
    public function __construct(
        public readonly string $body,
        public readonly array $headers = [],
        public readonly ?string $key = null,
        public readonly ?int $delayMs = null,
    ) {
        if ($delayMs !== null && $delayMs < 0) {
            throw new \InvalidArgumentException('Message delay must be zero or positive.');
        }
    }

    /**
     * @param array<mixed> $data
     * @param array<string, mixed> $headers
     */
    public static function json(array $data, array $headers = [], ?string $key = null, ?int $delayMs = null): self
    {
        return new self(
            (string) json_encode($data, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE),
            $headers,
            $key,
            $delayMs,
        );
    }
}
