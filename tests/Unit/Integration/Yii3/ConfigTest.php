<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Integration\Yii3;

use Erikwang2013\Messaging\Config\Config;
use Erikwang2013\Messaging\Manager;
use Erikwang2013\Messaging\Tests\Support\FakeDriverFactory;
use PHPUnit\Framework\TestCase;
use Psr\Container\ContainerInterface;

/**
 * No yiisoft packages are installed here; the config files only need a PSR-11 container.
 */
final class ConfigTest extends TestCase
{
    public function test_di_definition_builds_manager_from_params(): void
    {
        $params = [
            'erikwang2013/messaging-php' => [
                'default' => 'primary',
                'connections' => [
                    'primary' => ['driver' => 'fake'],
                ],
            ],
        ];

        $definitions = require dirname(__DIR__, 4) . '/config/di.php';
        $manager = $definitions[Manager::class]($this->container());

        self::assertInstanceOf(Manager::class, $manager);

        $manager->extend('fake', new FakeDriverFactory());
        $connection = $manager->connection();
        self::assertSame('primary', $connection->name());

        $connection->publish('orders', 'hello');
        self::assertSame('hello', $connection->receive('orders')?->body());
    }

    public function test_shipped_params_are_a_valid_manager_config(): void
    {
        $params = require dirname(__DIR__, 4) . '/config/params.php';
        $config = Config::fromArray($params['erikwang2013/messaging-php']);

        self::assertSame('default', $config->defaultConnection());
        self::assertNotSame('', $config->connection('default')->driver());
    }

    private function container(): ContainerInterface
    {
        return new class () implements ContainerInterface {
            public function get(string $id): mixed
            {
                throw new \RuntimeException(sprintf('Unexpected service [%s].', $id));
            }

            public function has(string $id): bool
            {
                return false;
            }
        };
    }
}
