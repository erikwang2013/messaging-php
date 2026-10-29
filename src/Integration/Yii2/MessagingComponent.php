<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Integration\Yii2;

use Erikwang2013\Messaging\Connection;
use Erikwang2013\Messaging\Manager;
use yii\base\Component;

/**
 * Yii2 application component that lazily builds one Manager from the component properties.
 *
 *     // config/web.php, config/console.php
 *     'components' => [
 *         'messaging' => [
 *             'class' => \Erikwang2013\Messaging\Integration\Yii2\MessagingComponent::class,
 *             'default' => 'primary',
 *             'connections' => [
 *                 'primary' => ['driver' => 'beanstalkd', 'host' => '127.0.0.1'],
 *             ],
 *         ],
 *     ],
 *
 *     Yii::$app->messaging->connection()->publish('orders', 'payload');
 *     Yii::$app->messaging->manager;   // the shared Manager, via getManager()
 *
 * `default` and `connections` are the same keys Manager accepts; leave `default`
 * unset to use the first connection.
 *
 * @property-read Manager $manager
 */
class MessagingComponent extends Component
{
    /** @var string|null Connection used by connection() when no name is given. */
    public ?string $default = null;

    /** @var array<string, array<string, mixed>> */
    public array $connections = [];

    private ?Manager $manager = null;

    public function getManager(): Manager
    {
        if ($this->manager === null) {
            $config = ['connections' => $this->connections];
            if ($this->default !== null && $this->default !== '') {
                $config['default'] = $this->default;
            }

            $this->manager = new Manager($config);
        }

        return $this->manager;
    }

    public function connection(?string $name = null): Connection
    {
        return $this->getManager()->connection($name);
    }
}
