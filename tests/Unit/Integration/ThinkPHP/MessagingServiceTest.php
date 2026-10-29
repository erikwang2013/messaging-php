<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Integration\ThinkPHP;

use Erikwang2013\Messaging\Integration\ThinkPHP\MessagingService;
use Erikwang2013\Messaging\Manager;
use Erikwang2013\Messaging\Tests\Support\FakeDriverFactory;
use PHPUnit\Framework\TestCase;
use think\App;
use think\Service;

final class MessagingServiceTest extends TestCase
{
    public function test_service_binds_manager_and_messaging_alias(): void
    {
        if (!class_exists(Service::class) || !class_exists(App::class)) {
            self::markTestSkipped('topthink/framework is not installed.');
        }

        $app = new App(sys_get_temp_dir() . '/messaging-thinkphp-' . uniqid('', true));
        $app->config->set(['messaging' => $this->config()]);

        (new MessagingService($app))->register();

        $manager = $app->make(Manager::class);
        $manager->extend('fake', new FakeDriverFactory());

        self::assertSame($manager, $app->make(Manager::class));
        self::assertSame($manager, $app->make('messaging'));
        self::assertSame('fake', $app->make('messaging')->connection()->name());
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
