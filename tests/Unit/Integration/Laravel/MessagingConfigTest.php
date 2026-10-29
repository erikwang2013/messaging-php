<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Integration\Laravel;

use Erikwang2013\Messaging\Config\Config;
use PHPUnit\Framework\TestCase;

final class MessagingConfigTest extends TestCase
{
    public function test_published_config_is_a_valid_manager_config(): void
    {
        $config = $this->config();
        $parsed = Config::fromArray($config);

        self::assertSame('redis', $config['connections']['redis']['driver']);
        self::assertSame('beanstalkd', $config['connections']['beanstalkd']['driver']);
        self::assertArrayHasKey($parsed->defaultConnection(), $config['connections']);
    }

    public function test_port_options_are_casted_to_int(): void
    {
        $config = $this->config();

        self::assertIsInt($config['connections']['redis']['port']);
        self::assertIsInt($config['connections']['redis']['database']);
        self::assertIsInt($config['connections']['beanstalkd']['port']);
    }

    /**
     * @return array<string, mixed>
     */
    private function config(): array
    {
        return require dirname(__DIR__, 4) . '/config/messaging.php';
    }
}
