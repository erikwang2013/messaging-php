<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Integration\Webman;

use Webman\Bootstrap as WebmanBootstrap;
use Workerman\Worker;

/**
 * Runs in every worker via config/plugin/erikwang2013/messaging-php/bootstrap.php.
 */
final class Bootstrap implements WebmanBootstrap
{
    public static function start(?Worker $worker): void
    {
        Messaging::boot((array) config(Messaging::CONFIG_KEY, []));
    }
}
