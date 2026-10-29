<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\ConsumeOptions;
use Erikwang2013\Messaging\Contract\DriverFactoryInterface;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Erikwang2013\Messaging\Manager;
use Erikwang2013\Messaging\Tests\Support\FakeDriver;
use Erikwang2013\Messaging\Tests\Support\FakeDriverFactory;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

final class ManagerTest extends TestCase
{
    public function test_publish_and_receive_through_connection(): void
    {
        $manager = $this->manager();
        $connection = $manager->connection();

        $result = $connection->publish('orders', Message::json(['id' => 1]));
        self::assertSame('orders', $result->destination);

        $message = $connection->receive('orders', new ReceiveOptions(100));
        self::assertNotNull($message);
        self::assertSame('{"id":1}', $message->body());
    }

    public function test_connections_are_cached(): void
    {
        $manager = $this->manager();

        self::assertSame($manager->connection('fake'), $manager->connection('fake'));
    }

    public function test_disconnect_forgets_named_connection(): void
    {
        $manager = $this->manager();
        $connection = $manager->connection('fake');

        $manager->disconnect('fake');

        self::assertNotSame($connection, $manager->connection('fake'));
    }

    public function test_disconnect_ignores_unknown_connection(): void
    {
        $manager = $this->manager();
        $connection = $manager->connection('fake');

        $manager->disconnect('unknown');

        self::assertSame($connection, $manager->connection('fake'));
    }

    public function test_disconnect_without_name_forgets_all_connections(): void
    {
        $manager = new Manager([
            'connections' => [
                'first' => ['driver' => 'fake'],
                'second' => ['driver' => 'fake'],
            ],
        ]);
        $manager->extend('fake', new FakeDriverFactory());

        $first = $manager->connection('first');
        $second = $manager->connection('second');

        $manager->disconnect();

        self::assertNotSame($first, $manager->connection('first'));
        self::assertNotSame($second, $manager->connection('second'));
    }

    public function test_unknown_driver_throws(): void
    {
        $manager = new Manager([
            'connections' => ['broken' => ['driver' => 'missing']],
        ]);

        $this->expectException(ConnectionException::class);

        $manager->connection();
    }

    public function test_publish_batch_requires_capability(): void
    {
        $manager = new Manager([
            'connections' => ['fake' => ['driver' => 'fake']],
        ]);
        $manager->extend('fake', new class () implements DriverFactoryInterface {
            public function create(ConnectionConfig $config, ?LoggerInterface $logger = null): DriverInterface
            {
                return new FakeDriver(new Capabilities(batchPublish: false));
            }
        });

        $this->expectException(UnsupportedFeatureException::class);

        $manager->connection()->publishBatch('orders', ['one', 'two']);
    }

    public function test_consume_processes_up_to_max_messages(): void
    {
        $manager = $this->manager();
        $connection = $manager->connection();
        $connection->publish('orders', 'one');
        $connection->publish('orders', 'two');
        $connection->publish('orders', 'three');

        $handled = [];
        $connection->consume(
            'orders',
            static function ($message) use (&$handled): void {
                $handled[] = $message->body();
                $message->ack();
            },
            new ConsumeOptions(timeoutMs: 10, maxMessages: 2),
        );

        self::assertSame(['one', 'two'], $handled);
        self::assertSame('three', $connection->receive('orders', new ReceiveOptions(10))?->body());
    }

    private function manager(): Manager
    {
        $manager = new Manager([
            'connections' => [
                'fake' => ['driver' => 'fake'],
            ],
        ]);
        $manager->extend('fake', new FakeDriverFactory());

        return $manager;
    }
}
