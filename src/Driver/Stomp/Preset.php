<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Stomp;

/**
 * Broker-specific STOMP behaviour shared by the ActiveMQ Classic and Artemis drivers.
 */
final class Preset
{
    public function __construct(
        public readonly string $name,
        public readonly int $defaultPort,
        public readonly bool $requeue,
        public readonly bool $delay,
        public readonly ?string $delayHeader,
        public readonly bool $delayIsAbsolute,
        public readonly bool $requiresActiveSubscription = false,
    ) {
    }
}
