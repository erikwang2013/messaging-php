<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Redis;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverFactoryInterface;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Driver\Redis\Resp\RespClient;
use Psr\Log\LoggerInterface;

final class RedisStreamsFactory implements DriverFactoryInterface
{
    public function create(ConnectionConfig $config, ?LoggerInterface $logger = null): DriverInterface
    {
        $host = $config->requireString('host');
        $port = $this->positiveInt($config, 'port', 6379);
        $database = $this->nonNegativeInt($config, 'database', 0);
        $timeout = $this->positiveFloat($config, 'timeout', 5.0);

        $password = $config->get('password');
        if ($password !== null && !is_string($password)) {
            throw $this->invalid($config, 'password', 'a string');
        }

        $maxLength = $config->get('max_length');
        if ($maxLength !== null && (!is_int($maxLength) || $maxLength <= 0)) {
            throw $this->invalid($config, 'max_length', 'a positive integer or null');
        }

        $group = $this->nonEmptyString($config, 'group', 'messaging');
        $consumer = $this->nonEmptyString($config, 'consumer', sprintf('%s-%d', gethostname() ?: 'php', getmypid()));

        $client = RespClient::connect($host, $port, $password, $database, $timeout);

        return new RedisStreamsDriver($client, $group, $consumer, $maxLength);
    }

    private function nonEmptyString(ConnectionConfig $config, string $key, string $default): string
    {
        $value = $config->get($key, $default);
        if (!is_string($value) || $value === '') {
            throw $this->invalid($config, $key, 'a non-empty string');
        }

        return $value;
    }

    private function positiveInt(ConnectionConfig $config, string $key, int $default): int
    {
        $value = $config->get($key, $default);
        if (!is_int($value) || $value <= 0) {
            throw $this->invalid($config, $key, 'a positive integer');
        }

        return $value;
    }

    private function nonNegativeInt(ConnectionConfig $config, string $key, int $default): int
    {
        $value = $config->get($key, $default);
        if (!is_int($value) || $value < 0) {
            throw $this->invalid($config, $key, 'a non-negative integer');
        }

        return $value;
    }

    private function positiveFloat(ConnectionConfig $config, string $key, float $default): float
    {
        $value = $config->get($key, $default);
        if (!is_int($value) && !is_float($value)) {
            throw $this->invalid($config, $key, 'a positive number');
        }

        if ($value <= 0) {
            throw $this->invalid($config, $key, 'a positive number');
        }

        return (float) $value;
    }

    private function invalid(ConnectionConfig $config, string $key, string $expected): \InvalidArgumentException
    {
        return new \InvalidArgumentException(sprintf(
            'Connection [%s] option [%s] must be %s.',
            $config->name(),
            $key,
            $expected,
        ));
    }
}
