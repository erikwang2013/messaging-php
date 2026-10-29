<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Contract;

final class PublishResult
{
    public function __construct(
        public string $destination,
        public ?string $id = null,
        public mixed $raw = null,
    ) {
    }
}
