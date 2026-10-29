<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Protocol\RabbitMq;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\ContentHeader;
use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\Frame;
use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\Protocol;
use Erikwang2013\Messaging\Driver\RabbitMq\RabbitMqFactory;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use PHPUnit\Framework\TestCase;

final class RabbitMqProtocolTest extends TestCase
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

    public function test_handshake_publish_receive_and_ack(): void
    {
        $driver = $this->createDriver($this->startServer('normal'));

        self::assertNull($driver->receive('orders', new ReceiveOptions(30)));

        $result = $driver->publish('orders', new Message('hello', ['trace' => 'abc'], 'key-1'));

        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame($result->id, $message->id());
        self::assertSame('key-1', $message->key());
        self::assertSame(['trace' => 'abc'], $message->headers());
        self::assertSame(1, $message->raw()['delivery_tag']);
        $message->ack();

        self::assertNull($driver->receive('orders', new ReceiveOptions(30)));
        $driver->close();

        $frames = $this->waitForFrames(12);
        $methods = $this->decodeMethods($frames);

        $startOk = $this->findMethod($methods, Protocol::CLASS_CONNECTION, Protocol::CONNECTION_START_OK);
        self::assertNotNull($startOk);
        self::assertSame('PLAIN', $startOk['args']['mechanism']);
        self::assertSame("\x00guest\x00guest", $startOk['args']['response']);
        self::assertSame('messaging-php', $startOk['args']['client_properties']['product']);

        $tuneOk = $this->findMethod($methods, Protocol::CLASS_CONNECTION, Protocol::CONNECTION_TUNE_OK);
        self::assertNotNull($tuneOk);
        self::assertSame(2047, $tuneOk['args']['channel_max']);
        self::assertSame(131072, $tuneOk['args']['frame_max']);
        self::assertSame(60, $tuneOk['args']['heartbeat']);

        $open = $this->findMethod($methods, Protocol::CLASS_CONNECTION, Protocol::CONNECTION_OPEN);
        self::assertNotNull($open);
        self::assertSame('/', $open['args']['virtual_host']);

        $channelOpen = $this->findMethod($methods, Protocol::CLASS_CHANNEL, Protocol::CHANNEL_OPEN);
        self::assertNotNull($channelOpen);
        self::assertSame(1, $channelOpen['channel']);

        $qos = $this->findMethod($methods, Protocol::CLASS_BASIC, Protocol::BASIC_QOS);
        self::assertNotNull($qos);
        self::assertSame(1, $qos['args']['prefetch_count']);

        $declare = $this->findMethod($methods, Protocol::CLASS_QUEUE, Protocol::QUEUE_DECLARE);
        self::assertNotNull($declare);
        self::assertSame('orders', $declare['args']['queue']);
        self::assertTrue($declare['args']['durable']);

        $consume = $this->findMethod($methods, Protocol::CLASS_BASIC, Protocol::BASIC_CONSUME);
        self::assertNotNull($consume);
        self::assertSame('orders', $consume['args']['queue']);
        self::assertFalse($consume['args']['no_ack']);

        $publish = $this->findMethod($methods, Protocol::CLASS_BASIC, Protocol::BASIC_PUBLISH);
        self::assertNotNull($publish);
        self::assertSame('', $publish['args']['exchange']);
        self::assertSame('orders', $publish['args']['routing_key']);

        $headerFrame = $this->findFrame($frames, Frame::TYPE_HEADER);
        self::assertNotNull($headerFrame);
        $header = ContentHeader::decode($headerFrame['payload']);
        self::assertSame(5, $header['body_size']);
        self::assertSame(['trace' => 'abc'], $header['properties']['headers']);
        self::assertSame(2, $header['properties']['delivery_mode']);
        self::assertSame('key-1', $header['properties']['correlation_id']);
        self::assertSame($result->id, $header['properties']['message_id']);

        $bodyFrame = $this->findFrame($frames, Frame::TYPE_BODY);
        self::assertNotNull($bodyFrame);
        self::assertSame('hello', $bodyFrame['payload']);

        $ack = $this->findMethod($methods, Protocol::CLASS_BASIC, Protocol::BASIC_ACK);
        self::assertNotNull($ack);
        self::assertSame(1, $ack['args']['delivery_tag']);
        self::assertFalse($ack['args']['multiple']);

        self::assertNotNull($this->findMethod($methods, Protocol::CLASS_CONNECTION, Protocol::CONNECTION_CLOSE));
    }

    public function test_reject_requeues_and_marks_redelivery(): void
    {
        $driver = $this->createDriver($this->startServer('normal'));

        $driver->publish('jobs', new Message('retry'));
        $first = $driver->receive('jobs', new ReceiveOptions(1000));
        self::assertNotNull($first);
        $first->reject(true);

        $second = $driver->receive('jobs', new ReceiveOptions(1000));
        self::assertNotNull($second);
        self::assertSame('retry', $second->body());
        self::assertTrue($second->raw()['redelivered']);
        $second->ack();

        $driver->close();

        $methods = $this->decodeMethods($this->waitForFrames(13));
        $nack = $this->findMethod($methods, Protocol::CLASS_BASIC, Protocol::BASIC_NACK);
        self::assertNotNull($nack);
        self::assertSame(1, $nack['args']['delivery_tag']);
        self::assertTrue($nack['args']['requeue']);
    }

    public function test_reject_without_requeue_drops_the_message(): void
    {
        $driver = $this->createDriver($this->startServer('normal'));

        $driver->publish('jobs', new Message('drop-me'));
        $message = $driver->receive('jobs', new ReceiveOptions(1000));
        self::assertNotNull($message);
        $message->reject(false);

        self::assertNull($driver->receive('jobs', new ReceiveOptions(30)));

        $driver->close();

        $methods = $this->decodeMethods($this->waitForFrames(12));
        $nack = $this->findMethod($methods, Protocol::CLASS_BASIC, Protocol::BASIC_NACK);
        self::assertNotNull($nack);
        self::assertFalse($nack['args']['requeue']);
    }

    public function test_receive_times_out_while_heartbeats_arrive(): void
    {
        $driver = $this->createDriver($this->startServer('heartbeat'));

        $start = microtime(true);
        $message = $driver->receive('empty', new ReceiveOptions(100));
        $elapsed = microtime(true) - $start;

        self::assertNull($message);
        self::assertGreaterThanOrEqual(0.08, $elapsed);
        self::assertLessThan(2.0, $elapsed);

        $driver->close();
    }

    public function test_malformed_frame_raises_protocol_exception(): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('frame-end');

        $this->createDriver($this->startServer('malformed'));
    }

    public function test_authentication_failure_raises_authentication_exception(): void
    {
        $this->expectException(AuthenticationException::class);
        $this->expectExceptionMessage('Login was refused');

        $this->createDriver($this->startServer('auth-error'));
    }

    private function createDriver(int $port): DriverInterface
    {
        return (new RabbitMqFactory())->create(ConnectionConfig::fromArray('rabbitmq', [
            'driver' => 'rabbitmq',
            'host' => '127.0.0.1',
            'port' => $port,
            'username' => 'guest',
            'password' => 'guest',
            'vhost' => '/',
            'timeout' => 2.0,
            'heartbeat' => 60,
        ]));
    }

    private function startServer(string $mode): int
    {
        $logFile = tempnam(sys_get_temp_dir(), 'amqp-protocol-');
        self::assertIsString($logFile);
        $this->logFile = $logFile;

        $process = proc_open(
            [PHP_BINARY, __DIR__ . '/fake_amqp_broker.php', $mode, $logFile],
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
        self::assertIsString($line, 'The fake AMQP server did not report a port: ' . $this->serverErrors());

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
     * @return list<array{type: int, channel: int, payload: string}>
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

            $frames[] = Frame::decode($decoded);
        }

        return $frames;
    }

    /**
     * @return list<array{type: int, channel: int, payload: string}>
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

        self::fail(sprintf('Expected at least %d logged AMQP frames, got %d.', $count, count($frames)));
    }

    /**
     * @param list<array{type: int, channel: int, payload: string}> $frames
     * @return list<array{class_id: int, method_id: int, args: array<string, mixed>, channel: int}>
     */
    private function decodeMethods(array $frames): array
    {
        $methods = [];
        foreach ($frames as $frame) {
            if ($frame['type'] !== Frame::TYPE_METHOD) {
                continue;
            }

            $method = Protocol::decodeMethod($frame['payload']);
            $method['channel'] = $frame['channel'];
            $methods[] = $method;
        }

        return $methods;
    }

    /**
     * @param list<array{class_id: int, method_id: int, args: array<string, mixed>, channel: int}> $methods
     * @return array{class_id: int, method_id: int, args: array<string, mixed>, channel: int}|null
     */
    private function findMethod(array $methods, int $classId, int $methodId): ?array
    {
        foreach ($methods as $method) {
            if ($method['class_id'] === $classId && $method['method_id'] === $methodId) {
                return $method;
            }
        }

        return null;
    }

    /**
     * @param list<array{type: int, channel: int, payload: string}> $frames
     * @return array{type: int, channel: int, payload: string}|null
     */
    private function findFrame(array $frames, int $type): ?array
    {
        foreach ($frames as $frame) {
            if ($frame['type'] === $type) {
                return $frame;
            }
        }

        return null;
    }
}
