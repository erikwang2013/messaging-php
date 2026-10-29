<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Nsq;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Driver\Nsq\NsqDriver;
use Erikwang2013\Messaging\Driver\Nsq\NsqFactory;
use PHPUnit\Framework\TestCase;

final class NsqFactoryTest extends TestCase
{
    public function test_create_builds_a_driver_from_minimal_config(): void
    {
        $driver = (new NsqFactory())->create($this->config(['host' => '127.0.0.1']));

        self::assertInstanceOf(NsqDriver::class, $driver);
    }

    public function test_host_is_required(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new NsqFactory())->create($this->config([]));
    }

    public function test_port_must_be_a_valid_integer(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new NsqFactory())->create($this->config(['host' => '127.0.0.1', 'port' => '4150']));
    }

    public function test_channel_must_be_a_valid_nsq_name(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new NsqFactory())->create($this->config(['host' => '127.0.0.1', 'channel' => 'bad channel']));
    }

    public function test_timeout_must_be_positive(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new NsqFactory())->create($this->config(['host' => '127.0.0.1', 'timeout' => 0]));
    }

    public function test_requeue_delay_must_not_be_negative(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new NsqFactory())->create($this->config(['host' => '127.0.0.1', 'requeue_delay_ms' => -1]));
    }

    /**
     * @param array<string, mixed> $options
     */
    private function config(array $options): ConnectionConfig
    {
        return ConnectionConfig::fromArray('nsq', ['driver' => 'nsq'] + $options);
    }
}
