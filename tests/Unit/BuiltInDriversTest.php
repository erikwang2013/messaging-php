<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit;

use Erikwang2013\Messaging\Contract\DriverFactoryInterface;
use Erikwang2013\Messaging\Manager;
use PHPUnit\Framework\TestCase;

final class BuiltInDriversTest extends TestCase
{
    public function test_all_twenty_built_in_driver_names_resolve(): void
    {
        $constant = (new \ReflectionClass(Manager::class))->getReflectionConstant('BUILT_IN_DRIVERS');
        self::assertNotFalse($constant);

        /** @var array<string, class-string<DriverFactoryInterface>> $drivers */
        $drivers = $constant->getValue();

        self::assertSame([
            'activemq',
            'artemis',
            'automq',
            'azure-service-bus',
            'beanstalkd',
            'confluent',
            'eventhubs',
            'gcp-pubsub',
            'kafka',
            'kinesis',
            'nats',
            'nsq',
            'pulsar',
            'rabbitmq',
            'redis',
            'redpanda',
            'rocketmq',
            'sns',
            'sqs',
            'zeromq',
        ], array_keys($drivers));

        foreach ($drivers as $name => $factory) {
            self::assertTrue(class_exists($factory), sprintf('Driver [%s] factory is missing.', $name));
            self::assertTrue(
                is_subclass_of($factory, DriverFactoryInterface::class),
                sprintf('Driver [%s] factory does not implement DriverFactoryInterface.', $name),
            );
        }
    }
}
