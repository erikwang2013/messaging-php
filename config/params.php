<?php

declare(strict_types=1);

/*
 * Default Yii3 params, merged by yiisoft/config (see composer.json extra.config-plugin).
 * Override the whole 'erikwang2013/messaging-php' block in the application's config/params.php.
 *
 * Add more entries under `connections` for multi-connection setups; pick one with
 * `$manager->connection('name')`. When MESSAGING_DRIVER changes, set MESSAGING_PORT
 * to that broker's port too (redis: 6379, nsq: 4150, nats: 4222, ...).
 */
$env = static function (string $key, string $default): string {
    $value = $_ENV[$key] ?? $_SERVER[$key] ?? getenv($key);

    return $value === false || !is_scalar($value) ? $default : (string) $value;
};

return [
    'erikwang2013/messaging-php' => [
        'default' => 'default',
        'connections' => [
            'default' => [
                'driver' => $env('MESSAGING_DRIVER', 'beanstalkd'),
                'host' => $env('MESSAGING_HOST', '127.0.0.1'),
                'port' => (int) $env('MESSAGING_PORT', '11300'),
                'timeout' => (float) $env('MESSAGING_TIMEOUT', '5.0'),
            ],
        ],
    ],
];
