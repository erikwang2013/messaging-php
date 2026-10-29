<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Beanstalkd;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Beanstalkd\BeanstalkdDriver;
use Erikwang2013\Messaging\Driver\Beanstalkd\BeanstalkdFactory;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class BeanstalkdDriverTest extends TestCase
{
    public function test_factory_creates_driver(): void
    {
        self::assertInstanceOf(BeanstalkdDriver::class, $this->driver());
    }

    public function test_capabilities_match_beanstalkd_semantics(): void
    {
        $capabilities = $this->driver()->capabilities();

        self::assertTrue($capabilities->ack);
        self::assertTrue($capabilities->requeue);
        self::assertTrue($capabilities->delay);
        self::assertFalse($capabilities->consumerGroups);
        self::assertFalse($capabilities->batchPublish);
        self::assertFalse($capabilities->ordering);
        self::assertFalse($capabilities->headers);
    }

    public function test_publish_rejects_headers(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publish('tube', new Message('body', ['trace' => 'abc']));
    }

    public function test_publish_rejects_message_keys(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publish('tube', new Message('body', [], 'key'));
    }

    public function test_publish_batch_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publishBatch('tube', [new Message('body')]);
    }

    public function test_publish_rejects_tube_names_with_protocol_characters(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver()->publish("tube\r\nuse other", new Message('body'));
    }

    public function test_receive_rejects_invalid_tube_names(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver()->receive('bad tube', new ReceiveOptions(10));
    }

    public function test_missing_host_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(['host' => '']);
    }

    public function test_invalid_port_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(['port' => 70000]);
    }

    public function test_invalid_priority_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(['priority' => 'high']);
    }

    public function test_invalid_connect_timeout_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(['timeout' => 0]);
    }

    /**
     * @param array<string, mixed> $options
     */
    private function driver(array $options = []): BeanstalkdDriver
    {
        $driver = (new BeanstalkdFactory())->create(ConnectionConfig::fromArray('test', [
            'driver' => 'beanstalkd',
            'host' => '127.0.0.1',
            ...$options,
        ]));

        self::assertInstanceOf(BeanstalkdDriver::class, $driver);

        return $driver;
    }
}
