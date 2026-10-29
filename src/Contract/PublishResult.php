<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Contract;

final class PublishResult
{
    public function __construct(
        public readonly string $destination,
        public readonly ?string $id = null,
        public readonly mixed $raw = null,
    ) {
    }
}
