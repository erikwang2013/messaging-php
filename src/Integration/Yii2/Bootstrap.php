<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Integration\Yii2;

use yii\base\BootstrapInterface;

/**
 * Registers the `messaging` component on every Yii2 request. Listed in composer.json
 * extra.bootstrap, so yiisoft/yii2-composer wires it without application config.
 */
final class Bootstrap implements BootstrapInterface
{
    public function bootstrap($app): void
    {
        if ($app->has('messaging')) {
            return;
        }

        $config = require __DIR__ . '/config/messaging.php';
        $app->set('messaging', $config['components']['messaging']);
    }
}
