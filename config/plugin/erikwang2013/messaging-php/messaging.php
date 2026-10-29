<?php

declare(strict_types=1);

// webman 在启动时通过 support/bootstrap.php 加载 .env，因此这里用 getenv()
return [
    'default' => getenv('MESSAGING_CONNECTION') ?: 'redis',

    'connections' => [
        'redis' => [
            'driver' => 'redis',
            'host' => getenv('MESSAGING_REDIS_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('MESSAGING_REDIS_PORT') ?: 6379),
            'password' => getenv('MESSAGING_REDIS_PASSWORD') ?: null,
            'database' => (int) (getenv('MESSAGING_REDIS_DB') ?: 0),
            'group' => getenv('MESSAGING_REDIS_GROUP') ?: 'messaging',
            'consumer' => getenv('MESSAGING_REDIS_CONSUMER') ?: null,
        ],

        'beanstalkd' => [
            'driver' => 'beanstalkd',
            'host' => getenv('MESSAGING_BEANSTALKD_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('MESSAGING_BEANSTALKD_PORT') ?: 11300),
        ],
    ],
];
