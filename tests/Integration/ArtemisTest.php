<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Integration;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Stomp\ArtemisFactory;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Erikwang2013\Messaging\Tests\Contract\DriverContractTestCase;
use PHPUnit\Framework\Attributes\Group;

#[Group('integration')]
final class ArtemisTest extends DriverContractTestCase
{
    private DriverInterface $mq;

    protected function createDriver(): DriverInterface
    {
        $config = ConnectionConfig::fromArray('artemis', [
            'driver' => 'artemis',
            'host' => getenv('MESSAGING_ARTEMIS_HOST') ?: '127.0.0.1',
            // docker-compose maps the Artemis STOMP acceptor (container 61613) to host 61614.
            'port' => getenv('MESSAGING_ARTEMIS_PORT') ?: 61614,
            // apache/artemis image defaults (ARTEMIS_USER / ARTEMIS_PASSWORD).
            'login' => getenv('MESSAGING_ARTEMIS_LOGIN') ?: 'artemis',
            'passcode' => getenv('MESSAGING_ARTEMIS_PASSCODE') ?: 'artemis',
            'timeout' => 5.0,
        ]);

        $driver = (new ArtemisFactory())->create($config);
        $this->mq = $driver;

        try {
            $driver->receive('integration-probe-' . bin2hex(random_bytes(4)), new ReceiveOptions(20));
        } catch (ConnectionException $exception) {
            $driver->close();
            self::markTestSkipped('Artemis is not available: ' . $exception->getMessage());
        }

        return $driver;
    }

    public function test_reject_requeue_throws_unsupported_feature(): void
    {
        self::assertFalse($this->mq->capabilities()->requeue);

        $destination = 'reject-' . bin2hex(random_bytes(6));
        $this->mq->receive($destination, new ReceiveOptions(1));
        $this->mq->publish($destination, new Message('nack-me'));
        $message = $this->mq->receive($destination, new ReceiveOptions(1000));
        self::assertNotNull($message);

        // Artemis treats STOMP NACK like ACK (ARTEMIS-1983); the message is not requeued.
        // Closing the consumer connection in tearDown is what makes the broker redeliver it.
        $this->expectException(UnsupportedFeatureException::class);
        $message->reject(true);
    }

    public function test_delay_is_honored(): void
    {
        if (!$this->mq->capabilities()->delay) {
            self::markTestSkipped('Artemis delay is not supported.');
        }

        $destination = 'delay-' . bin2hex(random_bytes(6));
        $this->mq->receive($destination, new ReceiveOptions(1));
        $this->mq->publish($destination, new Message('later', [], null, 300));

        self::assertNull($this->mq->receive($destination, new ReceiveOptions(100)));

        $message = $this->mq->receive($destination, new ReceiveOptions(5000));
        self::assertNotNull($message);
        self::assertSame('later', $message->body());
        $message->ack();
    }
}
