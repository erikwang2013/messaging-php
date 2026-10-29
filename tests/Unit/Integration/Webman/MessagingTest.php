<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Integration\Webman;

use Erikwang2013\Messaging\Integration\Webman\Messaging;
use Erikwang2013\Messaging\Tests\Support\FakeDriverFactory;
use PHPUnit\Framework\TestCase;

final class MessagingTest extends TestCase
{
    protected function tearDown(): void
    {
        Messaging::close();
    }

    public function test_boot_returns_manager_and_connection_uses_default(): void
    {
        $manager = Messaging::boot($this->config());
        $manager->extend('fake', new FakeDriverFactory());

        self::assertSame($manager, Messaging::instance());
        self::assertSame('fake', Messaging::connection()->name());

        Messaging::connection()->publish('orders', 'hello');
        self::assertSame('hello', Messaging::connection()->receive('orders')?->body());
    }

    public function test_instance_before_boot_throws(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('not booted');

        Messaging::instance();
    }

    public function test_close_forgets_the_manager(): void
    {
        Messaging::boot($this->config());
        Messaging::close();

        $this->expectException(\RuntimeException::class);

        Messaging::instance();
    }

    public function test_config_key_matches_webman_plugin_layout(): void
    {
        self::assertSame('plugin.erikwang2013.messaging-php.messaging', Messaging::CONFIG_KEY);
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
