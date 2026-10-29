<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Integration\Hyperf;

use Erikwang2013\Messaging\Config\Config;
use Erikwang2013\Messaging\Integration\Hyperf\ConfigProvider;
use Erikwang2013\Messaging\Integration\Hyperf\ManagerFactory;
use Erikwang2013\Messaging\Manager;
use PHPUnit\Framework\TestCase;

final class ConfigProviderTest extends TestCase
{
    public function test_registers_manager_factory_and_published_config(): void
    {
        if (!defined('BASE_PATH')) {
            define('BASE_PATH', '/app');
        }

        $provider = (new ConfigProvider())();

        self::assertSame(ManagerFactory::class, $provider['dependencies'][Manager::class]);

        $publish = $provider['publish'][0];
        self::assertSame('messaging', $publish['id']);
        self::assertSame('/app/config/autoload/messaging.php', $publish['destination']);
        self::assertFileExists($publish['source']);
    }

    public function test_shipped_config_is_a_valid_manager_config(): void
    {
        $raw = require dirname(__DIR__, 4) . '/src/Integration/Hyperf/config/messaging.php';
        $config = Config::fromArray($raw);

        self::assertSame('default', $config->defaultConnection());
        self::assertNotSame('', $config->connection('default')->driver());
        self::assertNotSame('', $raw['connections']['default']['host']);
        self::assertIsInt($raw['connections']['default']['port']);
        self::assertIsFloat($raw['connections']['default']['timeout']);
    }
}
