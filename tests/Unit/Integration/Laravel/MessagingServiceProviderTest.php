<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Integration\Laravel;

use Erikwang2013\Messaging\Integration\Laravel\Facades\Messaging as MessagingFacade;
use Erikwang2013\Messaging\Integration\Laravel\MessagingServiceProvider;
use Erikwang2013\Messaging\Manager;
use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Support\Facades\Facade;
use Illuminate\Support\ServiceProvider;
use PHPUnit\Framework\TestCase;

final class MessagingServiceProviderTest extends TestCase
{
    public function test_provider_registers_manager_as_singleton(): void
    {
        if (!class_exists(ServiceProvider::class) || !class_exists(Container::class) || !class_exists(Repository::class)) {
            self::markTestSkipped('laravel/framework is not installed.');
        }

        $app = new Container();
        $app->instance('config', new Repository(['messaging' => $this->config()]));

        (new MessagingServiceProvider($app))->register();

        $manager = $app->make(Manager::class);
        self::assertInstanceOf(Manager::class, $manager);
        self::assertSame($manager, $app->make(Manager::class));
        self::assertSame($manager, $app->make('messaging'));
    }

    public function test_facade_accessor_points_at_messaging_binding(): void
    {
        if (!class_exists(Facade::class)) {
            self::markTestSkipped('laravel/framework is not installed.');
        }

        $method = new \ReflectionMethod(MessagingFacade::class, 'getFacadeAccessor');
        if (\PHP_VERSION_ID < 80100) {
            $method->setAccessible(true);
        }

        self::assertSame('messaging', $method->invoke(null));
    }

    /**
     * @return array<string, mixed>
     */
    private function config(): array
    {
        return [
            'default' => 'fake',
            'connections' => [
                'fake' => ['driver' => 'fake'],
            ],
        ];
    }
}
