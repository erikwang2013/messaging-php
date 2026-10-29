<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Protocol\Pulsar;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Pulsar\PulsarFactory;
use Erikwang2013\Messaging\Exception\ProtocolException;
use PHPUnit\Framework\TestCase;

final class PulsarDriverProtocolTest extends TestCase
{
    private const TOPIC = 'persistent://public/default/orders';

    /** @var list<array{process: resource, pipes: array<int, resource>, log: string, port: int}> */
    private array $servers = [];

    protected function tearDown(): void
    {
        foreach ($this->servers as $server) {
            $this->stopServer($server);
        }

        $this->servers = [];
    }

    public function test_publish_and_receive_roundtrip(): void
    {
        $server = $this->startServer('basic');
        $driver = $this->driver($server['port']);

        $published = $driver->publish(self::TOPIC, new Message(
            'hello pulsar',
            ['trace' => 'abc', 'attempt' => 7, 'flag' => true],
            'key-1',
        ));

        self::assertSame(self::TOPIC, $published->destination);
        self::assertMatchesRegularExpression('/^1:\d+$/', (string) $published->id);

        $received = $driver->receive(self::TOPIC, new ReceiveOptions(2000));

        self::assertNotNull($received);
        self::assertSame('hello pulsar', $received->body());
        self::assertSame('key-1', $received->key());
        self::assertSame(['trace' => 'abc', 'attempt' => '7', 'flag' => 'true'], $received->headers());
        self::assertSame($published->id, $received->id());
        self::assertSame(1, $received->attempts());
        self::assertSame(self::TOPIC, $received->raw()['topic']);

        $received->ack();
        self::assertNull($driver->receive(self::TOPIC, new ReceiveOptions(200)));

        $publishedAt = microtime(true) * 1000;
        $driver->publish(self::TOPIC, new Message('later', [], null, 5000));
        $driver->close();

        $this->waitForCommand($server['log'], 'Pong');
        $this->waitForCommand($server['log'], 'Ack');

        $commands = $this->commands($server['log']);
        foreach (['Connect', 'Lookup', 'Producer', 'Send', 'Subscribe', 'Flow', 'Ack', 'Pong'] as $command) {
            self::assertContains($command, $commands, 'Expected Pulsar command ' . $command);
        }

        $sends = $this->entries($server['log'], 'command', 'Send');
        self::assertGreaterThanOrEqual((int) $publishedAt + 4000, $sends[1]['delay_at']);
    }

    public function test_reject_requeue_redelivers_the_same_message(): void
    {
        $server = $this->startServer('basic');
        $driver = $this->driver($server['port']);

        $driver->publish(self::TOPIC, new Message('retry me', [], 'retry-key'));

        $first = $driver->receive(self::TOPIC, new ReceiveOptions(2000));
        self::assertNotNull($first);
        self::assertSame(1, $first->attempts());
        $first->reject(true);

        $second = $driver->receive(self::TOPIC, new ReceiveOptions(2000));
        self::assertNotNull($second);
        self::assertSame($first->id(), $second->id());
        self::assertSame('retry me', $second->body());
        self::assertSame(2, $second->attempts());
        $second->reject(false);

        self::assertNull($driver->receive(self::TOPIC, new ReceiveOptions(200)));
        $driver->close();

        $this->waitForCommand($server['log'], 'Redeliver');

        self::assertContains('Redeliver', $this->commands($server['log']));
        self::assertContains('Ack', $this->commands($server['log']));
    }

    public function test_receive_timeout_returns_null_and_keeps_the_connection(): void
    {
        $server = $this->startServer('basic');
        $driver = $this->driver($server['port']);
        $started = microtime(true);

        self::assertNull($driver->receive(self::TOPIC, new ReceiveOptions(100)));
        $elapsed = microtime(true) - $started;
        self::assertGreaterThanOrEqual(0.05, $elapsed);
        self::assertLessThan(1.5, $elapsed);

        // The outstanding flow permit must deliver the later publish without a
        // new Subscribe, proving the connection survived the receive timeout.
        $driver->publish(self::TOPIC, new Message('late message'));

        $received = $driver->receive(self::TOPIC, new ReceiveOptions(2000));
        self::assertNotNull($received);
        self::assertSame('late message', $received->body());
        $received->ack();
        $driver->close();

        $commands = $this->commands($server['log']);
        self::assertSame(1, count(array_keys($commands, 'Subscribe', true)));
        self::assertSame(2, count(array_keys($commands, 'Connect', true)));
    }

    public function test_lookup_redirect_connects_to_the_owning_broker(): void
    {
        $server = $this->startServer('redirect');
        $driver = $this->driver($server['port']);

        $published = $driver->publish(self::TOPIC, new Message('redirected'));
        self::assertNotNull($published->id);

        $received = $driver->receive(self::TOPIC, new ReceiveOptions(2000));
        self::assertNotNull($received);
        self::assertSame('redirected', $received->body());
        $received->ack();
        $driver->close();

        $lookups = $this->entries($server['log'], 'command', 'Lookup');

        self::assertGreaterThanOrEqual(2, count($lookups));
        self::assertContains(false, array_column($lookups, 'authoritative'));
        self::assertContains(true, array_column($lookups, 'authoritative'));
        self::assertGreaterThanOrEqual(2, count($this->entries($server['log'], 'redirect')));
    }

    public function test_lookup_error_is_mapped_to_protocol_exception(): void
    {
        $server = $this->startServer('lookup_error');
        $driver = $this->driver($server['port']);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('TopicNotFound (11)');

        try {
            $driver->publish(self::TOPIC, new Message('never sent'));
        } finally {
            $driver->close();
        }
    }

    public function test_send_error_is_mapped_to_protocol_exception(): void
    {
        $server = $this->startServer('send_error');
        $driver = $this->driver($server['port']);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('ServiceNotReady (6)');

        try {
            $driver->publish(self::TOPIC, new Message('never stored'));
        } finally {
            $driver->close();
        }
    }

    public function test_malformed_response_frame_is_rejected(): void
    {
        $server = $this->startServer('malformed');
        $driver = $this->driver($server['port']);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Invalid Pulsar frame size');

        try {
            $driver->publish(self::TOPIC, new Message('never sent'));
        } finally {
            $driver->close();
        }
    }

    public function test_partial_frame_is_reassembled(): void
    {
        $server = $this->startServer('partial');
        $driver = $this->driver($server['port']);

        $published = $driver->publish(self::TOPIC, new Message('split frame'));
        self::assertNotNull($published->id);

        $received = $driver->receive(self::TOPIC, new ReceiveOptions(2000));
        self::assertNotNull($received);
        self::assertSame('split frame', $received->body());
        $received->ack();
        $driver->close();

        self::assertContains('Send', $this->commands($server['log']));
    }

    private function driver(int $port): DriverInterface
    {
        $config = ConnectionConfig::fromArray('pulsar-fake', [
            'driver' => 'pulsar',
            'service_url' => 'pulsar://127.0.0.1:' . $port,
            'subscription' => 'protocol-' . bin2hex(random_bytes(4)),
            'timeout' => 2.0,
        ]);

        return (new PulsarFactory())->create($config);
    }

    /**
     * @return array{process: resource, pipes: array<int, resource>, log: string, port: int}
     */
    private function startServer(string $scenario): array
    {
        $log = tempnam(sys_get_temp_dir(), 'pulsar-fake-');
        self::assertIsString($log);

        $process = proc_open(
            [PHP_BINARY, __DIR__ . '/fixtures/fake_pulsar_broker.php', $log, $scenario],
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
        );
        self::assertIsResource($process, 'Unable to start the fake Pulsar broker.');

        $line = fgets($pipes[1]);
        self::assertIsString($line, 'The fake Pulsar broker did not report its listening port.');
        self::assertMatchesRegularExpression('/^\d+$/', trim($line), $line);

        $server = [
            'process' => $process,
            'pipes' => $pipes,
            'log' => $log,
            'port' => (int) trim($line),
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

    private function waitForCommand(string $log, string $command): void
    {
        $deadline = microtime(true) + 2.0;

        do {
            if (in_array($command, $this->commands($log), true)) {
                return;
            }

            usleep(20000);
        } while (microtime(true) < $deadline);

        self::fail(sprintf(
            'Timed out waiting for Pulsar command [%s]. Log: %s',
            $command,
            json_encode($this->entries($log)) ?: '[]',
        ));
    }

    /**
     * @return list<string>
     */
    private function commands(string $log): array
    {
        $commands = [];
        foreach ($this->entries($log) as $entry) {
            if (($entry['event'] ?? null) === 'command' && isset($entry['command'])) {
                $commands[] = (string) $entry['command'];
            }
        }

        return $commands;
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function entries(string $log, ?string $event = null, ?string $command = null): array
    {
        if (!is_file($log)) {
            return [];
        }

        $entries = [];
        foreach (file($log, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [] as $line) {
            $entry = json_decode($line, true);
            if (!is_array($entry)) {
                continue;
            }

            if ($event !== null && ($entry['event'] ?? null) !== $event) {
                continue;
            }

            if ($command !== null && ($entry['command'] ?? null) !== $command) {
                continue;
            }

            $entries[] = $entry;
        }

        return $entries;
    }
}
