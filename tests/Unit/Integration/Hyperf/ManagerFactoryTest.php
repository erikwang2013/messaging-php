<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Integration\Hyperf;

use Erikwang2013\Messaging\Integration\Hyperf\ManagerFactory;
use Erikwang2013\Messaging\Manager;
use Erikwang2013\Messaging\Tests\Support\FakeDriverFactory;
use PHPUnit\Framework\TestCase;
use Psr\Container\ContainerInterface;

/**
 * Hyperf is not installed in this repository: a PSR-11 container stand-in exposes a fake
 * ConfigInterface, which is all ManagerFactory talks to.
 */
final class ManagerFactoryTest extends TestCase
{
    public function test_builds_manager_from_hyperf_config(): void
    {
        $manager = (new ManagerFactory())($this->container([
            'default' => 'primary',
            'connections' => [
                'primary' => ['driver' => 'fake'],
            ],
        ]));
        $manager->extend('fake', new FakeDriverFactory());

        $connection = $manager->connection();
        self::assertSame('primary', $connection->name());

        $connection->publish('orders', 'hello');
        self::assertSame('hello', $connection->receive('orders')?->body());
    }

    public function test_missing_messaging_config_falls_back_to_shipped_defaults(): void
    {
        $manager = (new ManagerFactory())($this->container(null));

        self::assertInstanceOf(Manager::class, $manager);
        self::assertSame('default', $manager->connection()->name());
    }

    /**
     * @param array<string, mixed>|null $messaging
     */
    private function container(?array $messaging): ContainerInterface
    {
        $values = $messaging === null ? [] : ['messaging' => $messaging];
        $configService = new class ($values) {
            /** @param array<string, mixed> $values */
            public function __construct(private array $values)
            {
            }

            public function get(string $key, mixed $default = null): mixed
            {
                return $this->values[$key] ?? $default;
            }
        };

        return new class ($configService) implements ContainerInterface {
            public function __construct(private object $config)
            {
            }

            public function get(string $id): mixed
            {
                if ($id !== 'Hyperf\Contract\ConfigInterface') {
                    throw new \RuntimeException(sprintf('Unexpected service [%s].', $id));
                }

                return $this->config;
            }

            public function has(string $id): bool
            {
                return $id === 'Hyperf\Contract\ConfigInterface';
            }
        };
    }
}
