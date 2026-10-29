<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Integration;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Driver\Redis\RedisStreamsFactory;
use Erikwang2013\Messaging\Driver\Redis\Resp\RespClient;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Tests\Contract\DriverContractTestCase;
use PHPUnit\Framework\Attributes\Group;

#[Group('integration')]
final class RedisStreamsTest extends DriverContractTestCase
{
    private string $host = '127.0.0.1';
    private int $port = 6379;
    private ?string $password = null;

    protected function createDriver(): DriverInterface
    {
        $this->host = getenv('MESSAGING_REDIS_HOST') ?: '127.0.0.1';
        $this->port = (int) (getenv('MESSAGING_REDIS_PORT') ?: 6379);
        $password = getenv('MESSAGING_REDIS_PASSWORD');
        $this->password = is_string($password) && $password !== '' ? $password : null;

        try {
            return (new RedisStreamsFactory())->create(ConnectionConfig::fromArray('redis', [
                'driver' => 'redis',
                'host' => $this->host,
                'port' => $this->port,
                'password' => $this->password,
                'group' => 'contract',
                'consumer' => 'contract-worker-' . getmypid(),
            ]));
        } catch (ConnectionException $exception) {
            self::markTestSkipped(sprintf(
                'Redis is not reachable at %s:%d: %s',
                $this->host,
                $this->port,
                $exception->getMessage(),
            ));
        }
    }

    protected function tearDown(): void
    {
        parent::tearDown();

        try {
            $client = RespClient::connect($this->host, $this->port, $this->password, 0, 2.0);
            $keys = $client->command('KEYS', 'contract-*');
            if (is_array($keys) && $keys !== []) {
                $client->command('DEL', ...array_map(static fn (mixed $key): string => (string) $key, $keys));
            }

            $client->close();
        } catch (\Throwable) {
            // Best-effort cleanup against shared test infrastructure.
        }
    }
}
