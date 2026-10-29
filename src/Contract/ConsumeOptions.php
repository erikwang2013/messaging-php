<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Contract;

final class ConsumeOptions
{
    public function __construct(
        public int $timeoutMs = 0,
        public ?int $maxMessages = null,
    ) {
        if ($timeoutMs < 0) {
            throw new \InvalidArgumentException('Consume timeout must be zero or positive.');
        }

        if ($maxMessages !== null && $maxMessages < 1) {
            throw new \InvalidArgumentException('Consume maxMessages must be at least 1.');
        }
    }
}
