<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Contract;

final class Capabilities
{
    public function __construct(
        public readonly bool $ack = true,
        public readonly bool $requeue = true,
        public readonly bool $delay = false,
        public readonly bool $consumerGroups = false,
        public readonly bool $batchPublish = false,
        public readonly bool $ordering = false,
        public readonly bool $headers = true,
    ) {
    }
}
