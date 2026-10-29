<?php

declare(strict_types=1);

use Erikwang2013\Messaging\Manager;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;

/** @var array $params Merged params, injected by yiisoft/config. */

/*
 * Manager is registered as a container service, so controllers and services simply
 * type-hint it. Connection objects are still obtained per name via
 * `$manager->connection('name')`; Connection itself is not container-registered
 * because it is bound to a connection name and driver.
 *
 * The PSR-3 logger is auto-wired when the application binds one; otherwise the
 * library's NullLogger is used.
 */
return [
    Manager::class => static function (ContainerInterface $container) use ($params): Manager {
        $config = $params['erikwang2013/messaging-php'] ?? [];
        if (!is_array($config)) {
            throw new \RuntimeException('The [erikwang2013/messaging-php] params must be an array.');
        }

        $logger = $container->has(LoggerInterface::class) ? $container->get(LoggerInterface::class) : null;

        return new Manager($config, $logger instanceof LoggerInterface ? $logger : null);
    },
];
