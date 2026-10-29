<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Integration;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Pulsar\PulsarEndpoint;
use Erikwang2013\Messaging\Driver\Pulsar\PulsarFactory;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Erikwang2013\Messaging\Tests\Contract\DriverContractTestCase;
use PHPUnit\Framework\Attributes\Group;

/**
 * Every test uses a fresh topic and subscription so replaying from the
 * earliest position cannot observe another test's messages.
 */
#[Group('integration')]
final class PulsarTest extends DriverContractTestCase
{
    private const RECEIVE_TIMEOUT_MS = 15000;

    private DriverInterface $driver;

    private string $topic;

    protected function setUp(): void
    {
        $this->driver = $this->createDriver();
        $this->topic = 'persistent://public/default/contract-' . bin2hex(random_bytes(6));

        $endpoint = PulsarEndpoint::fromUrl($this->serviceUrl());
        $socket = @stream_socket_client(
            'tcp://' . $endpoint->host() . ':' . $endpoint->port(),
            $errorCode,
            $errorMessage,
            0.5,
        );

        if ($socket === false) {
            self::markTestSkipped(sprintf(
                'Pulsar is not available at %s: %s (%d).',
                $this->serviceUrl(),
                $errorMessage,
                $errorCode,
            ));
        }

        fclose($socket);
    }

    protected function tearDown(): void
    {
        $this->driver->close();
    }

    protected function createDriver(): DriverInterface
    {
        return (new PulsarFactory())->create(ConnectionConfig::fromArray('pulsar', [
            'driver' => 'pulsar',
            'service_url' => $this->serviceUrl(),
            'subscription' => 'messaging-php-' . bin2hex(random_bytes(6)),
            'timeout' => 10.0,
        ]));
    }

    public function test_publish_and_receive_roundtrip(): void
    {
        $result = $this->driver->publish($this->topic, new Message('hello'));

        self::assertSame($this->topic, $result->destination);
        self::assertNotNull($result->id);

        $message = $this->driver->receive($this->topic, new ReceiveOptions(self::RECEIVE_TIMEOUT_MS));

        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame($result->id, $message->id());
        self::assertSame(1, $message->attempts());
        $message->ack();
    }

    public function test_ack_removes_message(): void
    {
        $this->driver->publish($this->topic, new Message('ack-me'));

        $message = $this->driver->receive($this->topic, new ReceiveOptions(self::RECEIVE_TIMEOUT_MS));
        self::assertNotNull($message);
        $message->ack();

        self::assertNull($this->driver->receive($this->topic, new ReceiveOptions(1000)));
    }

    public function test_reject_requeues_message(): void
    {
        $this->driver->publish($this->topic, new Message('retry-me'));

        $first = $this->driver->receive($this->topic, new ReceiveOptions(self::RECEIVE_TIMEOUT_MS));
        self::assertNotNull($first);
        $first->reject(true);

        $second = $this->driver->receive($this->topic, new ReceiveOptions(self::RECEIVE_TIMEOUT_MS));
        self::assertNotNull($second);
        self::assertSame('retry-me', $second->body());
        self::assertSame(2, $second->attempts());
        $second->ack();
    }

    public function test_receive_returns_null_on_timeout(): void
    {
        self::assertNull($this->driver->receive($this->topic, new ReceiveOptions(1000)));
    }

    public function test_headers_and_key_are_preserved(): void
    {
        $this->driver->publish($this->topic, new Message(
            'payload',
            ['trace' => 'abc', 'attempt' => 2],
            'key-1',
        ));

        $message = $this->driver->receive($this->topic, new ReceiveOptions(self::RECEIVE_TIMEOUT_MS));
        self::assertNotNull($message);
        self::assertSame('abc', $message->headers()['trace']);
        self::assertSame('2', $message->headers()['attempt']);
        self::assertSame('key-1', $message->key());
    }

    public function test_binary_body_is_preserved(): void
    {
        $body = "\x00\x01\xffbinary";
        $this->driver->publish($this->topic, new Message($body));

        $message = $this->driver->receive($this->topic, new ReceiveOptions(self::RECEIVE_TIMEOUT_MS));
        self::assertNotNull($message);
        self::assertSame($body, $message->body());
    }

    public function test_publish_delay_is_honored(): void
    {
        $this->driver->publish($this->topic, new Message('delayed', [], null, 500));

        self::assertNull($this->driver->receive($this->topic, new ReceiveOptions(150)));

        $message = $this->driver->receive($this->topic, new ReceiveOptions(self::RECEIVE_TIMEOUT_MS));
        self::assertNotNull($message);
        self::assertSame('delayed', $message->body());
        $message->ack();
    }

    public function test_batch_publish_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver->publishBatch($this->topic, [new Message('one')]);
    }

    private function serviceUrl(): string
    {
        $url = getenv('MESSAGING_PULSAR_URL');

        return $url !== false && $url !== '' ? $url : 'pulsar://127.0.0.1:6650';
    }
}
