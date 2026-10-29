<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Protocol\Redis;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Redis\RedisStreamsFactory;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class RedisStreamsProtocolTest extends TestCase
{
    /**
     * @var list<array{resource, array<int, resource>, string}>
     */
    private array $servers = [];

    protected function tearDown(): void
    {
        foreach ($this->servers as [$process, $pipes, $logFile]) {
            foreach ($pipes as $pipe) {
                if (is_resource($pipe)) {
                    fclose($pipe);
                }
            }

            if (is_resource($process)) {
                proc_terminate($process);
                proc_close($process);
            }

            @unlink($logFile);
        }

        $this->servers = [];
    }

    public function test_publish_receive_and_ack_roundtrip(): void
    {
        [$port, $logFile] = $this->startServer();
        $driver = $this->createDriver($port);

        $result = $driver->publish('orders', new Message('hello', ['trace' => 'abc', 'attempt' => 2], 'key-1'));
        self::assertSame('orders', $result->destination);
        self::assertSame('1700000000000-0', $result->id);

        $message = $driver->receive('orders', new ReceiveOptions(100));
        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame(['trace' => 'abc', 'attempt' => 2], $message->headers());
        self::assertSame('key-1', $message->key());
        self::assertSame('1700000000000-0', $message->id());
        self::assertSame(1, $message->attempts());
        $message->ack();

        self::assertNull($driver->receive('orders', new ReceiveOptions(100)));
        $driver->close();

        $commands = $this->commands($logFile);
        self::assertSame('XADD', $commands[0][0]);

        $headersIndex = array_search('headers', $commands[0], true);
        self::assertIsInt($headersIndex);
        self::assertSame(['trace' => 'abc', 'attempt' => 2], json_decode((string) $commands[0][$headersIndex + 1], true));

        self::assertSame(['XGROUP', 'CREATE', 'orders', 'workers', '0', 'MKSTREAM'], $commands[1]);
        self::assertSame(
            ['XREADGROUP', 'GROUP', 'workers', 'consumer-1', 'COUNT', '1', 'BLOCK', '100', 'STREAMS', 'orders', '>'],
            $commands[2],
        );
        self::assertSame(['XACK', 'orders', 'workers', '1700000000000-0'], $commands[3]);
        self::assertSame('XREADGROUP', $commands[4][0]);
    }

    public function test_reject_false_acks_and_reject_true_is_unsupported(): void
    {
        [$port, $logFile] = $this->startServer();
        $driver = $this->createDriver($port);

        $driver->publish('orders', new Message('retry-me'));
        $message = $driver->receive('orders', new ReceiveOptions(100));
        self::assertNotNull($message);

        try {
            $message->reject(true);
            self::fail('reject(true) should be unsupported.');
        } catch (UnsupportedFeatureException) {
        }

        $message->reject(false);
        $driver->close();

        $commands = $this->commands($logFile);
        self::assertSame(['XACK', 'orders', 'workers', '1700000000000-0'], $commands[count($commands) - 1]);
    }

    public function test_zero_timeout_is_sent_as_block_zero(): void
    {
        [$port, $logFile] = $this->startServer();
        $driver = $this->createDriver($port);

        self::assertNull($driver->receive('orders', new ReceiveOptions(0)));
        $driver->close();

        $commands = $this->commands($logFile);
        self::assertSame('XREADGROUP', $commands[1][0]);
        $blockIndex = array_search('BLOCK', $commands[1], true);
        self::assertIsInt($blockIndex);
        self::assertSame('0', $commands[1][$blockIndex + 1]);
    }

    public function test_busygroup_error_is_ignored_on_receive(): void
    {
        [$port, $logFile] = $this->startServer('busygroup');
        $driver = $this->createDriver($port);

        self::assertNull($driver->receive('orders', new ReceiveOptions(50)));
        $driver->close();

        $commands = $this->commands($logFile);
        self::assertSame('XGROUP', $commands[0][0]);
        self::assertSame('XREADGROUP', $commands[1][0]);
    }

    public function test_nogroup_reply_recreates_the_group_and_retries(): void
    {
        [$port, $logFile] = $this->startServer('nogroup');
        $driver = $this->createDriver($port);

        self::assertNull($driver->receive('orders', new ReceiveOptions(50)));
        $driver->close();

        $commands = $this->commands($logFile);
        self::assertCount(4, $commands);
        self::assertSame('XGROUP', $commands[0][0]);
        self::assertSame('XREADGROUP', $commands[1][0]);
        self::assertSame('XGROUP', $commands[2][0]);
        self::assertSame('XREADGROUP', $commands[3][0]);
    }

    public function test_malformed_reply_throws_protocol_exception(): void
    {
        [$port] = $this->startServer('malformed');
        $driver = $this->createDriver($port);

        $this->expectException(ProtocolException::class);

        try {
            $driver->receive('orders', new ReceiveOptions(100));
        } finally {
            $driver->close();
        }
    }

    public function test_auth_failure_throws_authentication_exception(): void
    {
        [$port] = $this->startServer('auth-failure');

        $this->expectException(AuthenticationException::class);

        $this->createDriver($port, ['password' => 'wrong']);
    }

    public function test_password_and_database_are_sent_on_connect(): void
    {
        [$port, $logFile] = $this->startServer();
        $driver = $this->createDriver($port, ['password' => 'secret', 'database' => 2]);

        $driver->publish('orders', new Message('hello'));
        $driver->close();

        $commands = $this->commands($logFile);
        self::assertSame(['AUTH', 'secret'], $commands[0]);
        self::assertSame(['SELECT', '2'], $commands[1]);
        self::assertSame('XADD', $commands[2][0]);
    }

    /**
     * @param array<string, mixed> $overrides
     */
    private function createDriver(int $port, array $overrides = []): DriverInterface
    {
        return (new RedisStreamsFactory())->create(ConnectionConfig::fromArray('redis', $overrides + [
            'driver' => 'redis',
            'host' => '127.0.0.1',
            'port' => $port,
            'group' => 'workers',
            'consumer' => 'consumer-1',
        ]));
    }

    /**
     * @return array{int, string}
     */
    private function startServer(string $scenario = 'echo'): array
    {
        $logFile = tempnam(sys_get_temp_dir(), 'redis-fake-');
        self::assertIsString($logFile);

        $process = proc_open(
            [PHP_BINARY, __DIR__ . '/fixtures/fake_redis_server.php', $scenario, $logFile],
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
        );
        self::assertIsResource($process);

        stream_set_timeout($pipes[1], 5);
        $line = fgets($pipes[1]);
        self::assertIsString($line);
        $port = (int) trim($line);
        self::assertGreaterThan(0, $port);

        $this->servers[] = [$process, $pipes, $logFile];

        return [$port, $logFile];
    }

    /**
     * @return list<list<string>>
     */
    private function commands(string $logFile): array
    {
        $commands = [];
        foreach (file($logFile, FILE_IGNORE_NEW_LINES) ?: [] as $line) {
            $command = json_decode($line, true);
            if (is_array($command)) {
                $commands[] = $command;
            }
        }

        return $commands;
    }
}
