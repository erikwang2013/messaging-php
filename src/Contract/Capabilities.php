<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Contract;

final class Capabilities
{
    public function __construct(
        public bool $ack = true,
        public bool $requeue = true,
        public bool $delay = false,
        public bool $consumerGroups = false,
        public bool $batchPublish = false,
        public bool $ordering = false,
        public bool $headers = true,
        public bool $requiresActiveSubscription = false,
    ) {
    }
}
