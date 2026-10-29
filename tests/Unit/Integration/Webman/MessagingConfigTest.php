<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Integration\Webman;

use Erikwang2013\Messaging\Config\Config;
use PHPUnit\Framework\TestCase;

final class MessagingConfigTest extends TestCase
{
    public function test_plugin_config_is_a_valid_manager_config(): void
    {
        $config = require $this->configDir() . '/messaging.php';
        $parsed = Config::fromArray($config);

        self::assertSame('redis', $config['connections']['redis']['driver']);
        self::assertSame('beanstalkd', $config['connections']['beanstalkd']['driver']);
        self::assertArrayHasKey($parsed->defaultConnection(), $config['connections']);
        self::assertIsInt($config['connections']['redis']['port']);
    }

    public function test_plugin_bootstrap_boots_messaging_under_webman(): void
    {
        $bootstrap = require $this->configDir() . '/bootstrap.php';

        self::assertSame([\Erikwang2013\Messaging\Integration\Webman\Bootstrap::class], $bootstrap);
    }

    public function test_plugin_is_enabled_so_webman_loads_the_config(): void
    {
        $app = require $this->configDir() . '/app.php';

        self::assertTrue($app['enable']);
    }

    private function configDir(): string
    {
        return dirname(__DIR__, 4) . '/config/plugin/erikwang2013/messaging-php';
    }
}
