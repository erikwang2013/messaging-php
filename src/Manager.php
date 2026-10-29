<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging;

use Erikwang2013\Messaging\Config\Config;
use Erikwang2013\Messaging\Contract\DriverFactoryInterface;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class Manager
{
    /** @var array<string, class-string<DriverFactoryInterface>> */
    private const BUILT_IN_DRIVERS = [
        'activemq' => \Erikwang2013\Messaging\Driver\Stomp\ActiveMqFactory::class,
        'artemis' => \Erikwang2013\Messaging\Driver\Stomp\ArtemisFactory::class,
        'beanstalkd' => \Erikwang2013\Messaging\Driver\Beanstalkd\BeanstalkdFactory::class,
        'redis' => \Erikwang2013\Messaging\Driver\Redis\RedisStreamsFactory::class,
        'nats' => \Erikwang2013\Messaging\Driver\Nats\NatsFactory::class,
        'nsq' => \Erikwang2013\Messaging\Driver\Nsq\NsqFactory::class,
    ];

    private readonly Config $config;
    private readonly LoggerInterface $logger;

    /** @var array<string, DriverFactoryInterface> */
    private array $factories = [];

    /** @var array<string, Connection> */
    private array $connections = [];

    /**
     * @param array<string, mixed>|Config $config
     */
    public function __construct(array|Config $config, ?LoggerInterface $logger = null)
    {
        $this->config = is_array($config) ? Config::fromArray($config) : $config;
        $this->logger = $logger ?? new NullLogger();
    }

    public function extend(string $driver, DriverFactoryInterface $factory): void
    {
        $this->factories[$driver] = $factory;
    }

    public function connection(?string $name = null): Connection
    {
        $name ??= $this->config->defaultConnection();

        return $this->connections[$name] ??= $this->createConnection($name);
    }

    private function createConnection(string $name): Connection
    {
        $connectionConfig = $this->config->connection($name);
        $driverName = $connectionConfig->driver();

        $factory = $this->factories[$driverName] ?? null;
        if ($factory === null) {
            $factoryClass = self::BUILT_IN_DRIVERS[$driverName] ?? null;
            if ($factoryClass === null || !class_exists($factoryClass)) {
                throw new ConnectionException(sprintf(
                    'Unsupported driver [%s]. Register it with Manager::extend().',
                    $driverName,
                ));
            }

            $factory = new $factoryClass();
        }

        return new Connection($name, $factory->create($connectionConfig, $this->logger));
    }

    public function close(): void
    {
        foreach ($this->connections as $connection) {
            $connection->close();
        }

        $this->connections = [];
    }
}
