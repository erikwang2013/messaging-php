<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Integration\Webman;

use Erikwang2013\Messaging\Connection;
use Erikwang2013\Messaging\Manager;
use Psr\Log\LoggerInterface;

/**
 * Process-local Manager holder. webman workers are long-lived and do not share
 * memory, so boot once per worker; the plugin bootstrap does it for you.
 */
final class Messaging
{
    public const CONFIG_KEY = 'plugin.erikwang2013.messaging-php.messaging';

    private static ?Manager $manager = null;

    /**
     * @param array<string, mixed> $config
     */
    public static function boot(array $config, ?LoggerInterface $logger = null): Manager
    {
        return self::$manager = new Manager($config, $logger);
    }

    public static function instance(): Manager
    {
        if (self::$manager === null) {
            throw new \RuntimeException(
                'Messaging is not booted. Copy config/plugin/erikwang2013/messaging-php into the webman project, '
                . 'or call Messaging::boot(config(Messaging::CONFIG_KEY, [])).',
            );
        }

        return self::$manager;
    }

    public static function connection(?string $name = null): Connection
    {
        return self::instance()->connection($name);
    }

    public static function close(): void
    {
        if (self::$manager !== null) {
            self::$manager->close();
            self::$manager = null;
        }
    }
}
