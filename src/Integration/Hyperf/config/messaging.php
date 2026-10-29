<?php

declare(strict_types=1);

/*
 * Default Hyperf config, published to config/autoload/messaging.php by
 * `php bin/hyperf.php vendor:publish erikwang2013/messaging-php`.
 *
 * Add more entries under `connections` for multi-connection setups; pick one with
 * `$manager->connection('name')`. When MESSAGING_DRIVER changes, set MESSAGING_PORT
 * to that broker's port too (redis: 6379, nsq: 4150, nats: 4222, ...).
 */
$env = static function (string $key, string $default): string {
    if (function_exists('env')) {
        $value = env($key, $default);
    } else {
        $value = $_ENV[$key] ?? $_SERVER[$key] ?? getenv($key);
    }

    return $value === false || !is_scalar($value) ? $default : (string) $value;
};

return [
    'default' => 'default',
    'connections' => [
        'default' => [
            'driver' => $env('MESSAGING_DRIVER', 'beanstalkd'),
            'host' => $env('MESSAGING_HOST', '127.0.0.1'),
            'port' => (int) $env('MESSAGING_PORT', '11300'),
            'timeout' => (float) $env('MESSAGING_TIMEOUT', '5.0'),
        ],
    ],
];
