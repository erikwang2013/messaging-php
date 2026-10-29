<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Integration\Hyperf;

use Erikwang2013\Messaging\Manager;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;

/**
 * Builds a Manager from the `messaging` key of config/autoload/messaging.php.
 * The array is handed to Manager unchanged, so its shape matches `new Manager([...])`.
 */
final class ManagerFactory
{
    public function __invoke(ContainerInterface $container): Manager
    {
        // String id instead of the ConfigInterface constant: this package does not depend on
        // hyperf/framework, so loading the class must not require Hyperf to be installed.
        $config = $container->get('Hyperf\Contract\ConfigInterface')->get('messaging', []);
        if (!is_array($config)) {
            throw new \RuntimeException('The [messaging] config must be an array.');
        }

        $logger = $container->has(LoggerInterface::class) ? $container->get(LoggerInterface::class) : null;

        return new Manager($config, $logger instanceof LoggerInterface ? $logger : null);
    }
}
