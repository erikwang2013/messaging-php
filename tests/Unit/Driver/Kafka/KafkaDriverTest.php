<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Kafka;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Driver\Kafka\AutoMqFactory;
use Erikwang2013\Messaging\Driver\Kafka\ConfluentFactory;
use Erikwang2013\Messaging\Driver\Kafka\EventHubsFactory;
use Erikwang2013\Messaging\Driver\Kafka\KafkaConfig;
use Erikwang2013\Messaging\Driver\Kafka\KafkaDriver;
use Erikwang2013\Messaging\Driver\Kafka\KafkaFactory;
use Erikwang2013\Messaging\Driver\Kafka\RedpandaFactory;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;

final class KafkaDriverTest extends TestCase
{
    public function test_capabilities(): void
    {
        $driver = $this->driver('kafka');
        $capabilities = $driver->capabilities();

        self::assertTrue($capabilities->ack);
        self::assertTrue($capabilities->requeue);
        self::assertFalse($capabilities->delay);
        self::assertTrue($capabilities->consumerGroups);
        self::assertTrue($capabilities->batchPublish);
        self::assertTrue($capabilities->ordering);
        self::assertTrue($capabilities->headers);
        self::assertFalse($capabilities->requiresActiveSubscription);

        $driver->close();
    }

    public function test_publish_rejects_delays(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver('kafka')->publish('orders', new Message('body', [], null, 100));
    }

    public function test_publish_batch_rejects_delays_and_empty_batches_need_no_broker(): void
    {
        $driver = $this->driver('kafka');
        self::assertSame([], $driver->publishBatch('orders', []));

        $this->expectException(UnsupportedFeatureException::class);
        $driver->publishBatch('orders', [new Message('body', [], null, 5)]);
    }

    public function test_factories_create_drivers_for_each_alias(): void
    {
        $factories = [
            'kafka' => new KafkaFactory(),
            'redpanda' => new RedpandaFactory(),
            'automq' => new AutoMqFactory(),
            'confluent' => new ConfluentFactory(),
            'eventhubs' => new EventHubsFactory(),
        ];

        foreach ($factories as $alias => $factory) {
            $driver = $factory->create(self::connection($alias, [
                'brokers' => ['broker'],
            ]));

            self::assertInstanceOf(KafkaDriver::class, $driver);
            self::assertSame($alias, $this->configOf($driver)->alias, $alias);
            $driver->close();
        }
    }

    public function test_factory_derives_the_alias_from_the_connection_driver(): void
    {
        $driver = (new KafkaFactory('kafka'))->create(self::connection('eventhubs', [
            'brokers' => ['namespace.servicebus.windows.net'],
        ]));

        $config = $this->configOf($driver);
        self::assertSame('eventhubs', $config->alias);
        self::assertSame(9093, $config->brokers[0]['port']);
    }

    public function test_event_hubs_alias_warns_about_missing_sasl(): void
    {
        $logger = new class () extends AbstractLogger {
            /** @var list<string> */
            public array $warnings = [];

            /**
             * @param mixed $level
             * @param string|\Stringable $message
             * @param array<string, mixed> $context
             */
            public function log($level, $message, array $context = []): void
            {
                if ($level === 'warning') {
                    $this->warnings[] = (string) $message;
                }
            }
        };

        (new EventHubsFactory())->create(self::connection('eventhubs', [
            'brokers' => ['namespace.servicebus.windows.net'],
        ]), $logger);

        self::assertCount(1, $logger->warnings);
        self::assertStringContainsString('SASL', $logger->warnings[0]);
    }

    private function driver(string $alias): KafkaDriver
    {
        return new KafkaDriver(KafkaConfig::fromConnection(self::connection($alias, [
            'brokers' => ['broker'],
            'timeout' => 1,
        ])));
    }

    private function configOf(KafkaDriver $driver): KafkaConfig
    {
        $property = new \ReflectionProperty(KafkaDriver::class, 'config');
        if (PHP_VERSION_ID < 80100) {
            $property->setAccessible(true);
        }

        return $property->getValue($driver);
    }

    /**
     * @param array<string, mixed> $options
     */
    private static function connection(string $driver, array $options): ConnectionConfig
    {
        return ConnectionConfig::fromArray('test', ['driver' => $driver] + $options);
    }
}
