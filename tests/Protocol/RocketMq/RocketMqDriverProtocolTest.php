<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Protocol\RocketMq;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\RocketMq\RocketMqDriver;
use Erikwang2013\Messaging\Driver\RocketMq\RocketMqFactory;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use PHPUnit\Framework\TestCase;

final class RocketMqDriverProtocolTest extends TestCase
{
    private string $group;
    private string $topic;

    /** @var resource|null */
    private $process = null;

    /** @var array<int, resource> */
    private array $pipes = [];

    private string $logFile = '';
    private int $nameserverPort = 0;
    private int $brokerPort = 0;

    protected function setUp(): void
    {
        $this->group = 'proto-' . bin2hex(random_bytes(4));
        $this->topic = 'orders-' . bin2hex(random_bytes(4));
        $this->logFile = (string) tempnam(sys_get_temp_dir(), 'rocketmq-log-');
    }

    protected function tearDown(): void
    {
        if ($this->process !== null) {
            proc_terminate($this->process);
            foreach ($this->pipes as $pipe) {
                fclose($pipe);
            }
            proc_close($this->process);
            $this->process = null;
            $this->pipes = [];
        }

        if ($this->logFile !== '' && is_file($this->logFile)) {
            unlink($this->logFile);
        }
    }

    public function test_publish_receive_and_ack_roundtrip(): void
    {
        $driver = $this->start('roundtrip');

        $result = $driver->publish($this->topic, new Message('hello', ['trace' => 'abc'], 'key-1'));
        self::assertSame($this->topic, $result->destination);
        self::assertNotNull($result->id);

        $message = $driver->receive($this->topic, new ReceiveOptions(1000));
        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame($result->id, $message->id());
        self::assertSame('key-1', $message->key());
        self::assertSame(['trace' => 'abc'], $message->headers());
        self::assertSame(1, $message->attempts());

        $message->ack();

        $lines = $this->logLines();
        self::assertNotEmpty(array_filter($lines, fn (string $line): bool => str_starts_with($line, 'ns:105 ')));
        self::assertNotEmpty(array_filter($lines, fn (string $line): bool => str_contains(
            $line,
            'broker:10 topic=' . $this->topic . ' queue=0 key=key-1 delay=- uniq=',
        )));
        self::assertNotEmpty(array_filter($lines, fn (string $line): bool => str_contains(
            $line,
            'broker:11 topic=' . $this->topic . ' queue=0 offset=0',
        )));
        self::assertNotEmpty(array_filter($lines, fn (string $line): bool => str_contains(
            $line,
            'broker:15 topic=' . $this->topic . ' queue=0 commit=1',
        )));
        self::assertGreaterThan(
            $this->indexOf($lines, 'broker:11 '),
            $this->indexOf($lines, 'broker:15 '),
        );

        self::assertNull($driver->receive($this->topic, new ReceiveOptions(100)));
    }

    public function test_publish_maps_delay_to_a_broker_delay_level(): void
    {
        $driver = $this->start('roundtrip');

        $driver->publish($this->topic, new Message('later', [], null, 2500));

        $lines = $this->logLines();
        self::assertNotEmpty(array_filter($lines, fn (string $line): bool => str_contains(
            $line,
            'broker:10 topic=' . $this->topic . ' queue=0 key=- delay=2 uniq=',
        )));
    }

    public function test_reject_requeue_sends_the_message_back_and_redelivers_it(): void
    {
        $driver = $this->start('roundtrip');

        $driver->publish($this->topic, new Message('retry-me', ['trace' => 'abc'], 'key-1'));
        $first = $driver->receive($this->topic, new ReceiveOptions(1000));
        self::assertNotNull($first);
        $first->reject(true);

        $lines = $this->logLines();
        self::assertNotEmpty(array_filter($lines, fn (string $line): bool => str_contains(
            $line,
            'broker:36 offset=1000 delay=1 origin=' . $this->topic,
        )));
        self::assertGreaterThan(
            $this->indexOf($lines, 'broker:36 '),
            $this->indexOf($lines, 'broker:15 topic=' . $this->topic . ' queue=0 commit=1'),
        );

        $second = $driver->receive($this->topic, new ReceiveOptions(2000));
        self::assertNotNull($second);
        self::assertSame('retry-me', $second->body());
        self::assertSame($first->id(), $second->id());
        self::assertSame('key-1', $second->key());
        self::assertSame(2, $second->attempts());
        $second->ack();

        $lines = $this->logLines();
        self::assertNotEmpty(array_filter($lines, fn (string $line): bool => str_contains(
            $line,
            'broker:15 topic=%RETRY%' . $this->group . ' queue=0 commit=1',
        )));
        self::assertNotEmpty(array_filter($lines, fn (string $line): bool => str_starts_with(
            $line,
            'broker:11 topic=%RETRY%' . $this->group,
        )));
    }

    public function test_reject_without_requeue_only_commits_the_offset(): void
    {
        $driver = $this->start('roundtrip');

        $driver->publish($this->topic, new Message('drop-me'));
        $message = $driver->receive($this->topic, new ReceiveOptions(1000));
        self::assertNotNull($message);
        $message->reject(false);

        $lines = $this->logLines();
        self::assertSame([], array_filter($lines, fn (string $line): bool => str_starts_with($line, 'broker:36 ')));
        self::assertNotEmpty(array_filter($lines, fn (string $line): bool => str_contains(
            $line,
            'broker:15 topic=' . $this->topic . ' queue=0 commit=1',
        )));
        self::assertNull($driver->receive($this->topic, new ReceiveOptions(100)));
    }

    public function test_publish_times_out_when_the_broker_does_not_answer(): void
    {
        $driver = $this->start('timeout');

        $started = microtime(true);
        try {
            $driver->publish($this->topic, new Message('hello'));
            self::fail('Expected a TimeoutException.');
        } catch (TimeoutException) {
            self::assertLessThan(3.0, microtime(true) - $started);
        }
    }

    public function test_malformed_response_frame_raises_a_protocol_exception(): void
    {
        $driver = $this->start('malformed');

        $this->expectException(ProtocolException::class);

        $driver->publish($this->topic, new Message('hello'));
    }

    public function test_receive_returns_null_when_the_pull_times_out(): void
    {
        $driver = $this->start('pull_timeout');

        $started = microtime(true);
        self::assertNull($driver->receive($this->topic, new ReceiveOptions(200)));
        self::assertLessThan(2.0, microtime(true) - $started);

        $lines = $this->logLines();
        self::assertNotEmpty(array_filter($lines, fn (string $line): bool => str_starts_with($line, 'broker:11 ')));
    }

    private function start(string $scenario): DriverInterface
    {
        $fixture = __DIR__ . '/fixtures/fake_rocketmq.php';
        $command = [PHP_BINARY, $fixture, $scenario, $this->logFile];

        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $process = proc_open($command, $descriptors, $pipes);
        self::assertIsResource($process, 'Unable to start the RocketMQ fixture.');

        $this->process = $process;
        $this->pipes = $pipes;

        stream_set_timeout($pipes[1], 5);
        $ready = fgets($pipes[1]);
        if ($ready === false) {
            $error = stream_get_contents($pipes[2]);
            self::fail('RocketMQ fixture did not start: ' . $error);
        }

        self::assertSame(1, preg_match('/\AREADY (\d+) (\d+)/', $ready, $matches));
        $this->nameserverPort = (int) $matches[1];
        $this->brokerPort = (int) $matches[2];

        $config = ConnectionConfig::fromArray('rocketmq', [
            'driver' => 'rocketmq',
            'nameserver' => '127.0.0.1:' . $this->nameserverPort,
            'group' => $this->group,
            'timeout' => 0.5,
            'consume_from' => 'first',
            'pull_batch_size' => 1,
        ]);

        $driver = (new RocketMqFactory())->create($config);
        self::assertInstanceOf(RocketMqDriver::class, $driver);

        return $driver;
    }

    /**
     * @return list<string>
     */
    private function logLines(): array
    {
        $contents = file_get_contents($this->logFile);
        self::assertIsString($contents);

        return array_values(array_filter(explode("\n", $contents), fn (string $line): bool => $line !== ''));
    }

    /**
     * @param list<string> $lines
     */
    private function indexOf(array $lines, string $prefix): int
    {
        foreach ($lines as $index => $line) {
            if (str_starts_with($line, $prefix)) {
                return $index;
            }
        }

        self::fail(sprintf('No log line starting with [%s].', $prefix));
    }
}
