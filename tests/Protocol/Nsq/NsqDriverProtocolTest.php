<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Protocol\Nsq;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Nsq\NsqFactory;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use PHPUnit\Framework\TestCase;

final class NsqDriverProtocolTest extends TestCase
{
    /** @var list<array{process: resource, pipes: array<int, resource>, log: string, port: int}> */
    private array $servers = [];

    protected function tearDown(): void
    {
        foreach ($this->servers as $server) {
            $this->stopServer($server);
        }

        $this->servers = [];
    }

    public function test_connect_sends_magic_and_publish_encodes_body(): void
    {
        $server = $this->startServer('publish');
        $driver = $this->driver($server['port']);

        $result = $driver->publish('events', new Message('hello'));
        $driver->close();

        self::assertSame('events', $result->destination);
        self::assertNull($result->id);
        self::assertSame(
            ['magic:  V2', 'command:PUB events', 'body:hello'],
            $this->events($server['log']),
        );
    }

    public function test_publish_reconnects_after_a_transport_failure(): void
    {
        $server = $this->startServer('reconnect');
        $driver = $this->driver($server['port']);

        try {
            $driver->publish('events', new Message('first'));
            self::fail('Expected the dropped connection to raise a ConnectionException.');
        } catch (ConnectionException) {
        }

        $result = $driver->publish('events', new Message('second'));
        $driver->close();

        self::assertSame('events', $result->destination);
        self::assertSame([
            'magic:  V2',
            'command:PUB events',
            'body:first',
            'magic:  V2',
            'command:PUB events',
            'body:second',
        ], $this->events($server['log']));
    }

    public function test_publish_error_frame_throws_protocol_exception(): void
    {
        $server = $this->startServer('error');
        $driver = $this->driver($server['port']);

        try {
            $driver->publish('events', new Message('hello'));
            self::fail('Expected a ProtocolException for the NSQ error frame.');
        } catch (ProtocolException $exception) {
            self::assertStringContainsString('E_INVALID', $exception->getMessage());
        } finally {
            $driver->close();
        }

        self::assertContains('command:PUB events', $this->events($server['log']));
    }

    public function test_subscribe_ready_message_and_finish(): void
    {
        $server = $this->startServer('receive');
        $driver = $this->driver($server['port']);

        $message = $driver->receive('events', new ReceiveOptions(2000));

        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame('0123456789abcdef', $message->id());
        self::assertSame(3, $message->attempts());
        self::assertSame(1234567890123, $message->raw()['timestamp']);

        $message->ack();
        $this->waitFor($server['log'], 'command:FIN 0123456789abcdef');
        $driver->close();

        self::assertSame([
            'magic:  V2',
            'command:SUB events default',
            'command:RDY 1',
            'command:FIN 0123456789abcdef',
        ], $this->events($server['log']));
    }

    public function test_reject_requeues_with_configured_delay(): void
    {
        $server = $this->startServer('receive');
        $driver = $this->driver($server['port'], ['requeue_delay_ms' => 2500]);

        $message = $driver->receive('events', new ReceiveOptions(2000));

        self::assertNotNull($message);
        $message->reject(true);
        $this->waitFor($server['log'], 'command:REQ 0123456789abcdef 2500');
        $driver->close();

        self::assertContains('command:REQ 0123456789abcdef 2500', $this->events($server['log']));
    }

    public function test_heartbeat_is_answered_with_nop_before_message(): void
    {
        $server = $this->startServer('heartbeat');
        $driver = $this->driver($server['port']);

        $message = $driver->receive('events', new ReceiveOptions(2000));

        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        $message->ack();
        $this->waitFor($server['log'], 'command:FIN 0123456789abcdef');
        $driver->close();

        $events = $this->events($server['log']);
        $nop = array_search('command:NOP', $events, true);
        $fin = array_search('command:FIN 0123456789abcdef', $events, true);

        self::assertIsInt($nop);
        self::assertIsInt($fin);
        self::assertLessThan($fin, $nop);
    }

    public function test_partial_frame_survives_a_receive_timeout(): void
    {
        $server = $this->startServer('partial');
        $driver = $this->driver($server['port']);

        self::assertNull($driver->receive('events', new ReceiveOptions(50)));

        $message = $driver->receive('events', new ReceiveOptions(2000));

        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        $message->ack();
        $driver->close();
    }

    public function test_receive_returns_null_on_timeout_and_keeps_the_connection(): void
    {
        $server = $this->startServer('timeout');
        $driver = $this->driver($server['port']);
        $started = microtime(true);

        self::assertNull($driver->receive('events', new ReceiveOptions(50)));
        self::assertLessThan(1.5, microtime(true) - $started);
        self::assertNull($driver->receive('events', new ReceiveOptions(50)));
        $driver->close();

        self::assertContains('command:SUB events default', $this->events($server['log']));
        self::assertContains('command:RDY 1', $this->events($server['log']));
    }

    /**
     * @param array<string, mixed> $options
     */
    private function driver(int $port, array $options = []): DriverInterface
    {
        $config = ConnectionConfig::fromArray('nsq', array_merge([
            'driver' => 'nsq',
            'host' => '127.0.0.1',
            'port' => $port,
            'timeout' => 1.0,
        ], $options));

        return (new NsqFactory())->create($config);
    }

    /**
     * @return array{process: resource, pipes: array<int, resource>, log: string, port: int}
     */
    private function startServer(string $scenario): array
    {
        $log = tempnam(sys_get_temp_dir(), 'nsqd-fake-');
        self::assertIsString($log);

        $process = proc_open(
            [PHP_BINARY, __DIR__ . '/fixtures/nsqd_fake.php', $scenario, $log],
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
        );
        self::assertIsResource($process);

        $line = fgets($pipes[1]);
        self::assertIsString($line, 'Fake nsqd did not report its listening port.');

        $port = (int) trim($line);
        self::assertGreaterThan(0, $port);

        $server = [
            'process' => $process,
            'pipes' => $pipes,
            'log' => $log,
            'port' => $port,
        ];
        $this->servers[] = $server;

        return $server;
    }

    /**
     * @param array{process: resource, pipes: array<int, resource>, log: string, port: int} $server
     */
    private function stopServer(array $server): void
    {
        $status = proc_get_status($server['process']);
        if ($status['running']) {
            proc_terminate($server['process']);
        }

        foreach ($server['pipes'] as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }

        proc_close($server['process']);

        if (is_file($server['log'])) {
            unlink($server['log']);
        }
    }

    /**
     * @return list<string>
     */
    private function events(string $log): array
    {
        $content = is_file($log) ? file_get_contents($log) : false;
        if ($content === false || $content === '') {
            return [];
        }

        return explode("\n", rtrim($content, "\n"));
    }

    private function waitFor(string $log, string $event): void
    {
        $deadline = microtime(true) + 2.0;

        while (microtime(true) < $deadline) {
            if (in_array($event, $this->events($log), true)) {
                return;
            }

            usleep(5_000);
        }

        self::fail(sprintf('Timed out waiting for fake nsqd event [%s].', $event));
    }
}
