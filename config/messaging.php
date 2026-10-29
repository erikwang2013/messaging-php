<?php

declare(strict_types=1);

// Laravel 用 env()，ThinkPHP 也用 env()；没有框架时退回 getenv()，便于单测直接 require
$env = static function (string $key, mixed $default = null): mixed {
    if (function_exists('env')) {
        return env($key, $default);
    }

    $value = getenv($key);

    return $value === false ? $default : $value;
};

return [
    'default' => $env('MESSAGING_CONNECTION', 'redis'),

    'connections' => [
        'redis' => [
            'driver' => 'redis',
            'host' => $env('MESSAGING_REDIS_HOST', '127.0.0.1'),
            'port' => (int) $env('MESSAGING_REDIS_PORT', 6379),
            'password' => $env('MESSAGING_REDIS_PASSWORD'),
            'database' => (int) $env('MESSAGING_REDIS_DB', 0),
            'group' => $env('MESSAGING_REDIS_GROUP', 'messaging'),
            'consumer' => $env('MESSAGING_REDIS_CONSUMER'),
        ],

        'beanstalkd' => [
            'driver' => 'beanstalkd',
            'host' => $env('MESSAGING_BEANSTALKD_HOST', '127.0.0.1'),
            'port' => (int) $env('MESSAGING_BEANSTALKD_PORT', 11300),
        ],
    ],
];
