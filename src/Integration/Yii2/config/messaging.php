<?php

declare(strict_types=1);

/*
 * Merge into the Yii2 app config:
 *
 *     $config = yii\helpers\ArrayHelper::merge($config, require __DIR__ . '/messaging.php');
 *
 * The component stays lazily loaded: no broker connection is opened until the first
 * connection() call.
 */
$env = static function (string $key, string $default): string {
    $value = $_ENV[$key] ?? $_SERVER[$key] ?? getenv($key);

    return $value === false || !is_scalar($value) ? $default : (string) $value;
};

return [
    'components' => [
        'messaging' => [
            'class' => \Erikwang2013\Messaging\Integration\Yii2\MessagingComponent::class,
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
    ],
];
