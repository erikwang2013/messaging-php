<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Stomp;

/**
 * Broker-specific STOMP behaviour shared by the ActiveMQ Classic and Artemis drivers.
 */
final class Preset
{
    public function __construct(
        public string $name,
        public int $defaultPort,
        public bool $requeue,
        public bool $delay,
        public ?string $delayHeader,
        public bool $delayIsAbsolute,
        public bool $requiresActiveSubscription = false,
    ) {
    }
}
