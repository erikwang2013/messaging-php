<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Integration;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Azure\ServiceBusFactory;
use Erikwang2013\Messaging\Tests\Contract\DriverContractTestCase;
use Erikwang2013\Messaging\Tests\Unit\Driver\Azure\ServiceBusMockTransport;
use GuzzleHttp\Client;
use GuzzleHttp\HandlerStack;

/**
 * The Service Bus emulator does not expose the REST data plane, so the shared
 * contract suite runs against an in-memory transport by default. Set
 * MESSAGING_AZURE_SERVICEBUS_NAMESPACE, _KEY_NAME, _KEY and _QUEUE to also run
 * the live test against a real namespace (the queue must already exist).
 */
final class ServiceBusTest extends DriverContractTestCase
{
    private const KEY_NAME = 'RootManageSharedAccessKey';
    private const KEY = 'dGVzdC1rZXktZm9yLXNlcnZpY2UtYnVz';

    protected function createDriver(): DriverInterface
    {
        $transport = new ServiceBusMockTransport(self::KEY_NAME, self::KEY);
        $stack = HandlerStack::create($transport);

        return (new ServiceBusFactory())->create(ConnectionConfig::fromArray('azure-contract', [
            'driver' => 'azure-service-bus',
            'namespace' => 'contract',
            'queue' => 'contract-default',
            'shared_access_key_name' => self::KEY_NAME,
            'shared_access_key' => self::KEY,
            'client' => new Client(['handler' => $stack]),
        ]));
    }

    public function test_live_publish_receive_and_ack(): void
    {
        $namespace = getenv('MESSAGING_AZURE_SERVICEBUS_NAMESPACE');
        $keyName = getenv('MESSAGING_AZURE_SERVICEBUS_KEY_NAME');
        $key = getenv('MESSAGING_AZURE_SERVICEBUS_KEY');
        $queue = getenv('MESSAGING_AZURE_SERVICEBUS_QUEUE');

        if (!is_string($namespace) || $namespace === ''
            || !is_string($keyName) || $keyName === ''
            || !is_string($key) || $key === ''
            || !is_string($queue) || $queue === ''
        ) {
            self::markTestSkipped(
                'Set MESSAGING_AZURE_SERVICEBUS_NAMESPACE, _KEY_NAME, _KEY and _QUEUE to run the live test.',
            );
        }

        $driver = (new ServiceBusFactory())->create(ConnectionConfig::fromArray('azure-live', [
            'driver' => 'azure-service-bus',
            'namespace' => $namespace,
            'queue' => $queue,
            'shared_access_key_name' => $keyName,
            'shared_access_key' => $key,
            'timeout' => 30.0,
        ]));

        $body = 'live-' . bin2hex(random_bytes(8));

        try {
            $result = $driver->publish($queue, new Message($body, ['trace' => 'live'], 'live-key'));
            self::assertNotNull($result->id);

            $deadline = microtime(true) + 20.0;
            $received = null;

            while (microtime(true) < $deadline) {
                $message = $driver->receive($queue, new ReceiveOptions(2000));
                if ($message === null) {
                    continue;
                }

                if ($message->body() === $body) {
                    $received = $message;
                    break;
                }

                // Leave unrelated backlog available for other consumers.
                $message->reject(true);
            }

            self::assertNotNull($received, 'The published message was not received before the deadline.');
            self::assertSame($result->id, $received->id());
            self::assertSame('live-key', $received->key());
            self::assertSame('live', $received->headers()['trace'] ?? null);
            $received->ack();
        } finally {
            $driver->close();
        }
    }
}
