<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Stomp;

final class ActiveMqPreset
{
    public static function create(): Preset
    {
        return new Preset(
            name: 'activemq',
            defaultPort: 61613,
            requeue: true,
            delay: true,
            // Relative delay in milliseconds; broker must run with schedulerSupport="true".
            delayHeader: 'AMQ_SCHEDULED_DELAY',
            delayIsAbsolute: false,
        );
    }
}
