<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Stomp;

final class ArtemisPreset
{
    public static function create(): Preset
    {
        return new Preset(
            name: 'artemis',
            defaultPort: 61613,
            // Artemis handles STOMP NACK like ACK (ARTEMIS-1983), so requeue is not available.
            requeue: false,
            delay: true,
            // Artemis STOMP maps this relative delay (ms) to its scheduled delivery time
            // (StompUtils); the AMQP-style _AMQ_SCHED_DELIVERY header is not recognised.
            delayHeader: 'AMQ_SCHEDULED_DELAY',
            delayIsAbsolute: false,
            // Artemis creates a queue when the first consumer attaches; a SEND to an address
            // without a queue yet is dropped, so publish only after subscribing.
            requiresActiveSubscription: true,
        );
    }
}
