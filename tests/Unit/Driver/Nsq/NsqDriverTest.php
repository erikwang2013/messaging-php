<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Nsq;

use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Driver\Nsq\NsqDriver;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class NsqDriverTest extends TestCase
{
    public function test_capabilities_match_the_implemented_protocol_scope(): void
    {
        $capabilities = $this->driver()->capabilities();

        self::assertTrue($capabilities->ack);
        self::assertTrue($capabilities->requeue);
        self::assertFalse($capabilities->delay);
        self::assertFalse($capabilities->consumerGroups);
        self::assertFalse($capabilities->batchPublish);
        self::assertFalse($capabilities->ordering);
        self::assertFalse($capabilities->headers);
    }

    public function test_publish_rejects_headers(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publish('orders', new Message('payload', ['trace' => 'abc']));
    }

    public function test_publish_rejects_keys(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publish('orders', new Message('payload', [], 'key-1'));
    }

    public function test_publish_rejects_delays(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publish('orders', new Message('payload', [], null, 100));
    }

    public function test_publish_batch_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publishBatch('orders', [new Message('one')]);
    }

    private function driver(): NsqDriver
    {
        // Port 1 is never dialed: every assertion here fails before the connection is opened.
        return new NsqDriver('127.0.0.1', 1, 'default', 0.1);
    }
}
