<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Contract;

final class ReceiveOptions
{
    /**
     * A zero timeout blocks until a message arrives or the connection closes.
     */
    public function __construct(public int $timeoutMs = 0)
    {
        if ($timeoutMs < 0) {
            throw new \InvalidArgumentException('Receive timeout must be zero or positive.');
        }
    }
}
