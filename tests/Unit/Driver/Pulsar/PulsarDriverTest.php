<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Pulsar;

use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Pulsar\PulsarDriver;
use Erikwang2013\Messaging\Driver\Pulsar\PulsarEndpoint;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class PulsarDriverTest extends TestCase
{
    public function test_capabilities_match_the_implemented_protocol_scope(): void
    {
        $capabilities = $this->driver()->capabilities();

        self::assertTrue($capabilities->ack);
        self::assertTrue($capabilities->requeue);
        self::assertTrue($capabilities->delay);
        self::assertTrue($capabilities->consumerGroups);
        self::assertFalse($capabilities->batchPublish);
        self::assertTrue($capabilities->ordering);
        self::assertTrue($capabilities->headers);
        self::assertFalse($capabilities->requiresActiveSubscription);
    }

    public function test_publish_batch_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publishBatch('persistent://public/default/events', [new Message('one')]);
    }

    public function test_publish_requires_a_destination_when_no_default_topic_is_configured(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('topic');

        $this->driver()->publish('', new Message('payload'));
    }

    public function test_receive_requires_a_destination_when_no_default_topic_is_configured(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('topic');

        $this->driver()->receive('', new ReceiveOptions(10));
    }

    public function test_close_without_connections_is_a_no_op(): void
    {
        $driver = $this->driver();

        $driver->close();
        $driver->close();

        self::assertTrue($driver->capabilities()->ack);
    }

    private function driver(): PulsarDriver
    {
        // Port 1 is never dialed: every assertion here fails before the connection is opened.
        return new PulsarDriver(
            PulsarEndpoint::fromUrl('pulsar://127.0.0.1:1'),
            null,
            'messaging',
            null,
            0.1,
        );
    }
}
