<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Protocol\Stomp;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Stomp\ActiveMqFactory;
use Erikwang2013\Messaging\Driver\Stomp\ArtemisFactory;
use Erikwang2013\Messaging\Driver\Stomp\StompFrame;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class StompProtocolTest extends TestCase
{
    /** @var resource|null */
    private $process = null;

    /** @var array<int, resource> */
    private array $pipes = [];

    private ?string $logFile = null;

    protected function tearDown(): void
    {
        $this->stopServer();
    }

    public function test_connect_publish_and_receive_roundtrip(): void
    {
        $driver = $this->createDriver('activemq', $this->startServer('echo'));

        self::assertNull($driver->receive('orders', new ReceiveOptions(30)));
        $result = $driver->publish('orders', new Message('hello', ['trace' => 'abc'], 'key-1'));
        $message = $driver->receive('orders', new ReceiveOptions(1000));

        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame($result->id, $message->id());
        self::assertSame('key-1', $message->key());
        self::assertSame(['trace' => 'abc'], $message->headers());

        $frames = $this->waitForFrames(3);

        self::assertSame('STOMP', $frames[0]->command);
        self::assertSame('1.2', $frames[0]->header('accept-version'));
        self::assertSame('127.0.0.1', $frames[0]->header('host'));
        self::assertSame('0,0', $frames[0]->header('heart-beat'));
        self::assertSame('admin', $frames[0]->header('login'));

        self::assertSame('SUBSCRIBE', $frames[1]->command);
        self::assertSame('/queue/orders', $frames[1]->header('destination'));
        self::assertSame('client-individual', $frames[1]->header('ack'));

        self::assertSame('SEND', $frames[2]->command);
        self::assertSame('/queue/orders', $frames[2]->header('destination'));
        self::assertSame('5', $frames[2]->header('content-length'));
        self::assertSame('abc', $frames[2]->header('trace'));
        self::assertSame('key-1', $frames[2]->header('correlation-id'));
        self::assertSame($result->id, $frames[2]->header('x-messaging-id'));

        $driver->close();
    }

    public function test_binary_body_and_typed_headers_roundtrip(): void
    {
        $driver = $this->createDriver('activemq', $this->startServer('echo'));

        $body = "\x00\x01\xffbinary";
        $headers = ['trace' => 'abc', 'attempt' => 2, 'flag' => true, 'ratio' => 1.5];
        $driver->publish('binary', new Message($body, $headers));

        $message = $driver->receive('binary', new ReceiveOptions(1000));

        self::assertNotNull($message);
        self::assertSame($body, $message->body());
        self::assertSame($headers, $message->headers());

        $driver->close();
    }

    public function test_ack_and_requeue_use_the_message_ack_id(): void
    {
        $driver = $this->createDriver('activemq', $this->startServer('echo'));
        $driver->receive('jobs', new ReceiveOptions(30));

        $driver->publish('jobs', new Message('one'));
        $first = $driver->receive('jobs', new ReceiveOptions(1000));
        self::assertNotNull($first);
        $first->ack();

        $driver->publish('jobs', new Message('two'));
        $second = $driver->receive('jobs', new ReceiveOptions(1000));
        self::assertNotNull($second);
        $second->reject(false);

        $driver->publish('jobs', new Message('three'));
        $third = $driver->receive('jobs', new ReceiveOptions(1000));
        self::assertNotNull($third);
        $third->reject(true);

        $requeued = $driver->receive('jobs', new ReceiveOptions(1000));
        self::assertNotNull($requeued);
        self::assertSame('three', $requeued->body());
        self::assertSame(2, $requeued->attempts());
        $requeued->ack();

        $frames = $this->waitForFrames(10);
        $acks = array_values(array_filter($frames, static fn (StompFrame $frame): bool => $frame->command === 'ACK'));
        $sends = array_values(array_filter($frames, static fn (StompFrame $frame): bool => $frame->command === 'SEND'));
        $nacks = array_values(array_filter($frames, static fn (StompFrame $frame): bool => $frame->command === 'NACK'));

        self::assertCount(4, $acks);
        self::assertSame('ack-1', $acks[0]->header('id'));
        self::assertSame('ack-2', $acks[1]->header('id'));
        self::assertSame('ack-3', $acks[2]->header('id'));
        self::assertSame('ack-4', $acks[3]->header('id'));
        // ActiveMQ NACK would poison the message into the DLQ (or discard it), so reject(true)
        // re-sends the message and then ACKs the original delivery.
        self::assertCount(4, $sends);
        self::assertSame('/queue/jobs', $sends[3]->header('destination'));
        self::assertSame('three', $sends[3]->body);
        self::assertSame('2', $sends[3]->header('x-messaging-attempts'));
        self::assertCount(0, $nacks);

        $driver->close();
    }

    public function test_artemis_reject_requeue_is_unsupported(): void
    {
        $driver = $this->createDriver('artemis', $this->startServer('echo'));
        $driver->publish('jobs', new Message('nack-me'));
        $message = $driver->receive('jobs', new ReceiveOptions(1000));
        self::assertNotNull($message);

        try {
            $message->reject(true);
            self::fail('Artemis must reject requeue requests.');
        } catch (UnsupportedFeatureException $exception) {
            self::assertStringContainsString('ARTEMIS-1983', $exception->getMessage());
        }

        $driver->close();
        $commands = array_map(static fn (StompFrame $frame): string => $frame->command, $this->waitForFrames(4));
        self::assertNotContains('NACK', $commands);
    }

    public function test_receive_returns_null_on_timeout(): void
    {
        $driver = $this->createDriver('activemq', $this->startServer('silent'));

        $start = microtime(true);
        $message = $driver->receive('empty', new ReceiveOptions(100));
        $elapsed = microtime(true) - $start;

        self::assertNull($message);
        self::assertGreaterThanOrEqual(0.08, $elapsed);
        self::assertLessThan(2.0, $elapsed);

        $driver->close();
    }

    public function test_connect_error_raises_authentication_exception(): void
    {
        $driver = $this->createDriver('activemq', $this->startServer('connect-error'));

        $this->expectException(AuthenticationException::class);
        $this->expectExceptionMessage('Unable to validate user');

        $driver->publish('jobs', new Message('nope'));
    }

    public function test_subscribe_error_raises_protocol_exception(): void
    {
        $driver = $this->createDriver('activemq', $this->startServer('subscribe-error'));

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('subscription rejected');

        $driver->receive('jobs', new ReceiveOptions(1000));
    }

    public function test_error_after_send_is_raised_on_the_next_receive(): void
    {
        $driver = $this->createDriver('activemq', $this->startServer('send-error'));
        self::assertNull($driver->receive('jobs', new ReceiveOptions(30)));

        $driver->publish('jobs', new Message('boom'));

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('send rejected');

        $driver->receive('jobs', new ReceiveOptions(1000));
    }

    public function test_destination_prefixing_and_passthrough(): void
    {
        $driver = $this->createDriver('activemq', $this->startServer('echo'), ['prefix' => 'custom-']);

        $driver->publish('orders', new Message('one'));
        $driver->publish('/queue/raw', new Message('two'));
        $driver->publish('/topic/news', new Message('three'));

        $sends = array_values(array_filter(
            $this->waitForFrames(4),
            static fn (StompFrame $frame): bool => $frame->command === 'SEND',
        ));

        self::assertCount(3, $sends);
        self::assertSame('custom-orders', $sends[0]->header('destination'));
        self::assertSame('/queue/raw', $sends[1]->header('destination'));
        self::assertSame('/topic/news', $sends[2]->header('destination'));

        $driver->close();
    }

    public function test_activemq_delay_uses_the_scheduled_delay_header(): void
    {
        $driver = $this->createDriver('activemq', $this->startServer('echo'));
        $driver->publish('jobs', new Message('later', [], null, 250));

        $send = $this->findSend($this->waitForFrames(2));

        self::assertNotNull($send);
        self::assertSame('250', $send->header('AMQ_SCHEDULED_DELAY'));

        $driver->close();
    }

    public function test_artemis_delay_uses_the_scheduled_delay_header(): void
    {
        $driver = $this->createDriver('artemis', $this->startServer('echo'));
        $driver->publish('jobs', new Message('later', [], null, 500));

        $send = $this->findSend($this->waitForFrames(2));

        self::assertNotNull($send);
        self::assertSame('500', $send->header('AMQ_SCHEDULED_DELAY'));

        $driver->close();
    }

    /**
     * @param array<string, mixed> $options
     */
    private function createDriver(string $broker, int $port, array $options = []): DriverInterface
    {
        $config = ConnectionConfig::fromArray($broker, array_merge([
            'driver' => $broker,
            'host' => '127.0.0.1',
            'port' => $port,
            'login' => 'admin',
            'passcode' => 'admin',
            'timeout' => 2.0,
        ], $options));

        return $broker === 'artemis'
            ? (new ArtemisFactory())->create($config)
            : (new ActiveMqFactory())->create($config);
    }

    private function startServer(string $mode): int
    {
        $logFile = tempnam(sys_get_temp_dir(), 'stomp-protocol-');
        self::assertIsString($logFile);
        $this->logFile = $logFile;

        $process = proc_open(
            [PHP_BINARY, __DIR__ . '/fake_stomp_server.php', $mode, $logFile],
            [
                0 => ['pipe', 'r'],
                1 => ['pipe', 'w'],
                2 => ['pipe', 'w'],
            ],
            $pipes,
        );
        self::assertIsResource($process);
        $this->process = $process;
        $this->pipes = $pipes;

        stream_set_timeout($pipes[1], 5);
        $line = fgets($pipes[1]);
        self::assertIsString($line, 'The fake STOMP server did not report a port: ' . $this->serverErrors());

        return (int) trim($line);
    }

    private function stopServer(): void
    {
        foreach ($this->pipes as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }
        $this->pipes = [];

        if (is_resource($this->process)) {
            proc_terminate($this->process);
            proc_close($this->process);
        }
        $this->process = null;

        if ($this->logFile !== null && is_file($this->logFile)) {
            unlink($this->logFile);
        }
        $this->logFile = null;
    }

    private function serverErrors(): string
    {
        $stream = $this->pipes[2] ?? null;
        if (!is_resource($stream)) {
            return '';
        }

        stream_set_blocking($stream, false);
        $errors = stream_get_contents($stream);
        stream_set_blocking($stream, true);

        return is_string($errors) ? $errors : '';
    }

    /**
     * @return list<StompFrame>
     */
    private function loggedFrames(): array
    {
        if ($this->logFile === null || !is_file($this->logFile)) {
            return [];
        }

        $frames = [];
        $content = (string) file_get_contents($this->logFile);
        foreach (explode("\n", $content) as $line) {
            if ($line === '') {
                continue;
            }

            $decoded = base64_decode($line, true);
            if ($decoded === false) {
                continue;
            }

            $frames[] = StompFrame::decode($decoded);
        }

        return $frames;
    }

    /**
     * @return list<StompFrame>
     */
    private function waitForFrames(int $count): array
    {
        $deadline = microtime(true) + 2.0;

        do {
            $frames = $this->loggedFrames();
            if (count($frames) >= $count) {
                return $frames;
            }

            usleep(5000);
        } while (microtime(true) < $deadline);

        self::fail(sprintf('Expected at least %d logged STOMP frames, got %d.', $count, count($frames)));
    }

    /**
     * @param list<StompFrame> $frames
     */
    private function findSend(array $frames): ?StompFrame
    {
        foreach ($frames as $frame) {
            if ($frame->command === 'SEND') {
                return $frame;
            }
        }

        return null;
    }
}
