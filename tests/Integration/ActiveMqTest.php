<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Integration;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Stomp\ActiveMqFactory;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Tests\Contract\DriverContractTestCase;
use PHPUnit\Framework\Attributes\Group;

#[Group('integration')]
final class ActiveMqTest extends DriverContractTestCase
{
    private DriverInterface $mq;

    protected function createDriver(): DriverInterface
    {
        $config = ConnectionConfig::fromArray('activemq', [
            'driver' => 'activemq',
            'host' => getenv('MESSAGING_ACTIVEMQ_HOST') ?: '127.0.0.1',
            'port' => getenv('MESSAGING_ACTIVEMQ_PORT') ?: 61613,
            'login' => getenv('MESSAGING_ACTIVEMQ_LOGIN') ?: 'admin',
            'passcode' => getenv('MESSAGING_ACTIVEMQ_PASSCODE') ?: 'admin',
            'timeout' => 5.0,
        ]);

        $driver = (new ActiveMqFactory())->create($config);
        $this->mq = $driver;

        try {
            $driver->receive('integration-probe-' . bin2hex(random_bytes(4)), new ReceiveOptions(20));
        } catch (ConnectionException $exception) {
            $driver->close();
            self::markTestSkipped('ActiveMQ is not available: ' . $exception->getMessage());
        }

        return $driver;
    }

    public function test_delay_is_honored(): void
    {
        if (!$this->mq->capabilities()->delay) {
            self::markTestSkipped('ActiveMQ delay is not supported.');
        }

        $destination = 'delay-' . bin2hex(random_bytes(6));
        $this->mq->publish($destination, new Message('later', [], null, 300));

        self::assertNull($this->mq->receive($destination, new ReceiveOptions(100)));

        $message = $this->mq->receive($destination, new ReceiveOptions(5000));
        self::assertNotNull($message);
        self::assertSame('later', $message->body());
        $message->ack();
    }
}
