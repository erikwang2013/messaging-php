<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Config;

use Erikwang2013\Messaging\Config\Config;
use PHPUnit\Framework\TestCase;

final class ConfigTest extends TestCase
{
    public function test_first_connection_is_default_when_omitted(): void
    {
        $config = Config::fromArray([
            'connections' => [
                'first' => ['driver' => 'fake'],
                'second' => ['driver' => 'fake'],
            ],
        ]);

        self::assertSame('first', $config->defaultConnection());
        self::assertSame('fake', $config->connection('first')->driver());
    }

    public function test_explicit_default_is_respected(): void
    {
        $config = Config::fromArray([
            'default' => 'second',
            'connections' => [
                'first' => ['driver' => 'fake'],
                'second' => ['driver' => 'fake'],
            ],
        ]);

        self::assertSame('second', $config->defaultConnection());
    }

    public function test_missing_connections_throws(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        Config::fromArray([]);
    }

    public function test_unknown_default_throws(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        Config::fromArray([
            'default' => 'missing',
            'connections' => ['first' => ['driver' => 'fake']],
        ]);
    }

    public function test_connection_without_driver_throws(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        Config::fromArray(['connections' => ['first' => []]]);
    }

    public function test_connection_options_and_required_string(): void
    {
        $config = Config::fromArray([
            'connections' => [
                'first' => ['driver' => 'fake', 'host' => '127.0.0.1', 'port' => 1234],
            ],
        ]);

        $connection = $config->connection('first');
        self::assertSame('127.0.0.1', $connection->requireString('host'));
        self::assertSame(1234, $connection->get('port'));
        self::assertSame('fallback', $connection->get('missing', 'fallback'));
    }
}
