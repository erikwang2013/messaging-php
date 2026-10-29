<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Integration\Hyperf;

use Erikwang2013\Messaging\Manager;

/**
 * Hyperf auto-discovery entry point. Add this class to composer.json:
 *
 *     "extra": { "hyperf": { "config": "Erikwang2013\\Messaging\\Integration\\Hyperf\\ConfigProvider" } }
 *
 * Hyperf then resolves Manager through {@see ManagerFactory} and exposes the shipped
 * config file for `vendor:publish`.
 */
class ConfigProvider
{
    public function __invoke(): array
    {
        return [
            'dependencies' => [
                Manager::class => ManagerFactory::class,
            ],
            'publish' => [
                [
                    'id' => 'messaging',
                    'description' => 'Messaging config',
                    'source' => __DIR__ . '/config/messaging.php',
                    // BASE_PATH is always defined inside Hyperf; the fallback keeps standalone tooling working.
                    'destination' => (defined('BASE_PATH') ? BASE_PATH : getcwd()) . '/config/autoload/messaging.php',
                ],
            ],
        ];
    }
}
