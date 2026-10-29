<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Protocol\Beanstalkd;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Beanstalkd\BeanstalkdFactory;
use Erikwang2013\Messaging\Exception\ProtocolException;
use PHPUnit\Framework\TestCase;

final class BeanstalkdProtocolTest extends TestCase
{
    private const FIXTURE = __DIR__ . '/fixtures/fake-broker.php';

    /** @var resource|null */
    private $process = null;

    /** @var array<int, resource> */
    private array $pipes = [];

    private ?DriverInterface $driver = null;

    protected function tearDown(): void
    {
        $this->driver?->close();
        $this->driver = null;

        if (is_resource($this->process)) {
            proc_terminate($this->process);
        }

        foreach ($this->pipes as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }
        $this->pipes = [];

        if (is_resource($this->process)) {
            proc_close($this->process);
            $this->process = null;
        }
    }

    public function test_publish_and_receive_roundtrip(): void
    {
        $driver = $this->brokerDriver('normal');

        $result = $driver->publish('proto-tube', new Message('hello'));
        self::assertSame('proto-tube', $result->destination);
        self::assertSame('1', $result->id);

        $message = $driver->receive('proto-tube', new ReceiveOptions(1000));
        self::assertNotNull($message);
        self::assertSame('1', $message->id());
        self::assertSame('hello', $message->body());
    }

    public function test_ack_deletes_message(): void
    {
        $driver = $this->brokerDriver('normal');
        $driver->publish('proto-tube', new Message('ack-me'));

        $message = $driver->receive('proto-tube', new ReceiveOptions(1000));
        self::assertNotNull($message);
        $message->ack();

        self::assertNull($driver->receive('proto-tube', new ReceiveOptions(1000)));
    }

    public function test_reject_with_requeue_releases_message(): void
    {
        $driver = $this->brokerDriver('normal');
        $driver->publish('proto-tube', new Message('retry-me'));

        $first = $driver->receive('proto-tube', new ReceiveOptions(1000));
        self::assertNotNull($first);
        $first->reject(true);

        $second = $driver->receive('proto-tube', new ReceiveOptions(1000));
        self::assertNotNull($second);
        self::assertSame('retry-me', $second->body());
    }

    public function test_reject_without_requeue_deletes_message(): void
    {
        $driver = $this->brokerDriver('normal');
        $driver->publish('proto-tube', new Message('drop-me'));

        $message = $driver->receive('proto-tube', new ReceiveOptions(1000));
        self::assertNotNull($message);
        $message->reject(false);

        self::assertNull($driver->receive('proto-tube', new ReceiveOptions(1000)));
    }

    public function test_receive_returns_null_after_broker_timed_out(): void
    {
        $driver = $this->brokerDriver('timeout');

        // The fixture only answers TIMED_OUT when the timeout was converted to 2 seconds.
        self::assertNull($driver->receive('proto-tube', new ReceiveOptions(1500)));
    }

    public function test_unexpected_status_is_reported_as_protocol_error(): void
    {
        $driver = $this->brokerDriver('malformed');

        $this->expectException(ProtocolException::class);

        $driver->publish('proto-tube', new Message('hello'));
    }

    private function brokerDriver(string $mode): DriverInterface
    {
        $this->driver = (new BeanstalkdFactory())->create(ConnectionConfig::fromArray('protocol', [
            'driver' => 'beanstalkd',
            'host' => '127.0.0.1',
            'port' => $this->startBroker($mode),
            'timeout' => 2.0,
        ]));

        return $this->driver;
    }

    private function startBroker(string $mode): int
    {
        $process = proc_open(
            [PHP_BINARY, self::FIXTURE, $mode],
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
        if ($line === false) {
            self::fail('Fake broker did not report its port: ' . (string) stream_get_contents($pipes[2]));
        }

        $port = (int) trim($line);
        self::assertGreaterThan(0, $port);

        return $port;
    }
}
