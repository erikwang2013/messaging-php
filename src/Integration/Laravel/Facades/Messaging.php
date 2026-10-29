<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Integration\Laravel\Facades;

use Erikwang2013\Messaging\Connection;
use Illuminate\Support\Facades\Facade;

/**
 * @method static Connection connection(?string $name = null)
 * @method static void close()
 */
final class Messaging extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return 'messaging';
    }
}
