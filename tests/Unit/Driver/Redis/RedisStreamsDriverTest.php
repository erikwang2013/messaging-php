<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Redis;

use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Driver\Redis\RedisStreamsDriver;
use Erikwang2013\Messaging\Driver\Redis\Resp\RespClient;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class RedisStreamsDriverTest extends TestCase
{
    /** @var resource|null */
    private $server = null;

    protected function tearDown(): void
    {
        if (is_resource($this->server)) {
            fclose($this->server);
        }
    }

    public function test_capabilities_match_streams_behavior(): void
    {
        $driver = $this->createDriver();
        $capabilities = $driver->capabilities();

        self::assertTrue($capabilities->ack);
        self::assertFalse($capabilities->requeue);
        self::assertFalse($capabilities->delay);
        self::assertTrue($capabilities->consumerGroups);
        self::assertTrue($capabilities->batchPublish);
        self::assertTrue($capabilities->ordering);
        self::assertTrue($capabilities->headers);

        $driver->close();
    }

    public function test_delayed_messages_are_rejected(): void
    {
        $driver = $this->createDriver();

        $this->expectException(UnsupportedFeatureException::class);

        try {
            $driver->publish('orders', new Message('late', delayMs: 5));
        } finally {
            $driver->close();
        }
    }

    public function test_batch_validates_every_message_before_publishing(): void
    {
        $driver = $this->createDriver();

        $this->expectException(UnsupportedFeatureException::class);

        try {
            $driver->publishBatch('orders', [
                new Message('first'),
                new Message('late', delayMs: 5),
            ]);
        } finally {
            $driver->close();
        }
    }

    private function createDriver(): RedisStreamsDriver
    {
        $this->server = stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
        self::assertIsResource($this->server, $errorMessage);

        $address = stream_socket_get_name($this->server, false);
        self::assertIsString($address);
        $separator = strrpos($address, ':');
        self::assertNotFalse($separator);

        $client = RespClient::connect(
            substr($address, 0, $separator),
            (int) substr($address, $separator + 1),
            null,
            0,
            1.0,
        );

        return new RedisStreamsDriver($client, 'workers', 'consumer-1');
    }
}
