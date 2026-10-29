<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Contract;

use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;

final class ReceivedMessage
{
    /**
     * @param array<string, mixed> $headers
     * @param (\Closure(): void)|null $ackCallback
     * @param (\Closure(bool): void)|null $rejectCallback
     */
    public function __construct(
        private readonly string $body,
        private readonly array $headers = [],
        private readonly ?string $id = null,
        private readonly ?string $key = null,
        private readonly int $attempts = 1,
        private readonly ?\Closure $ackCallback = null,
        private readonly ?\Closure $rejectCallback = null,
        private readonly mixed $raw = null,
    ) {
    }

    public function body(): string
    {
        return $this->body;
    }

    /**
     * @return array<string, mixed>
     */
    public function headers(): array
    {
        return $this->headers;
    }

    public function id(): ?string
    {
        return $this->id;
    }

    public function key(): ?string
    {
        return $this->key;
    }

    public function attempts(): int
    {
        return $this->attempts;
    }

    public function ack(): void
    {
        if ($this->ackCallback === null) {
            throw new UnsupportedFeatureException('This message cannot be acknowledged by the driver.');
        }

        ($this->ackCallback)();
    }

    public function reject(bool $requeue = false): void
    {
        if ($this->rejectCallback === null) {
            throw new UnsupportedFeatureException('This message cannot be rejected by the driver.');
        }

        ($this->rejectCallback)($requeue);
    }

    public function raw(): mixed
    {
        return $this->raw;
    }
}
