<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Redis;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Driver\Redis\RedisStreamsFactory;
use PHPUnit\Framework\TestCase;

final class RedisStreamsFactoryTest extends TestCase
{
    public function test_host_is_required(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('[host]');

        (new RedisStreamsFactory())->create(ConnectionConfig::fromArray('redis', ['driver' => 'redis']));
    }

    public function test_invalid_max_length_is_rejected_before_connecting(): void
    {
        $config = ConnectionConfig::fromArray('redis', [
            'driver' => 'redis',
            'host' => '127.0.0.1',
            'port' => 1,
            'max_length' => 0,
        ]);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('[max_length]');

        (new RedisStreamsFactory())->create($config);
    }

    public function test_invalid_database_is_rejected(): void
    {
        $config = ConnectionConfig::fromArray('redis', [
            'driver' => 'redis',
            'host' => '127.0.0.1',
            'database' => -1,
        ]);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('[database]');

        (new RedisStreamsFactory())->create($config);
    }

    public function test_invalid_group_is_rejected(): void
    {
        $config = ConnectionConfig::fromArray('redis', [
            'driver' => 'redis',
            'host' => '127.0.0.1',
            'group' => '',
        ]);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('[group]');

        (new RedisStreamsFactory())->create($config);
    }
}
