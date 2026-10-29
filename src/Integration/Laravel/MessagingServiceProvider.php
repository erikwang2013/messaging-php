<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Integration\Laravel;

use Erikwang2013\Messaging\Manager;
use Illuminate\Support\ServiceProvider;
use Psr\Log\LoggerInterface;

final class MessagingServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../../../config/messaging.php', 'messaging');

        $this->app->singleton(Manager::class, static function ($app): Manager {
            return new Manager(
                (array) $app['config']['messaging'],
                $app->bound(LoggerInterface::class) ? $app->make(LoggerInterface::class) : null,
            );
        });

        $this->app->alias(Manager::class, 'messaging');
    }

    public function boot(): void
    {
        $this->publishes([
            __DIR__ . '/../../../config/messaging.php' => config_path('messaging.php'),
        ], 'messaging-config');
    }
}
