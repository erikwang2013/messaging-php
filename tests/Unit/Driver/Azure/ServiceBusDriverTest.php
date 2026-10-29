<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Azure;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Azure\ServiceBusDriver;
use Erikwang2013\Messaging\Driver\Azure\ServiceBusFactory;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\RequestException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use GuzzleHttp\Client;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

final class ServiceBusDriverTest extends TestCase
{
    private const KEY_NAME = 'key-name';
    private const KEY = 'dGVzdC1rZXktZm9yLXNlcnZpY2UtYnVz';
    private const ENDPOINT = 'https://example.servicebus.windows.net';

    private ServiceBusMockTransport $transport;

    /** @var list<array{request: RequestInterface, response: \Psr\Http\Message\ResponseInterface}> */
    private array $history = [];

    public function test_publish_and_receive_roundtrip_preserves_headers_and_key(): void
    {
        $driver = $this->createDriver();

        $result = $driver->publish('orders', new Message('hello', ['trace' => 'abc', 'attempt' => 2], 'key-1'));

        self::assertSame('orders', $result->destination);
        self::assertMatchesRegularExpression('/^[0-9a-f]{32}$/', (string) $result->id);

        $request = $this->history[0]['request'];
        self::assertSame('POST', $request->getMethod());
        self::assertSame('/orders/messages', $request->getUri()->getPath());
        self::assertSame('text/plain', $request->getHeaderLine('Content-Type'));
        self::assertSame('hello', (string) $request->getBody());
        self::assertStringStartsWith(
            'SharedAccessSignature sr=https%3A%2F%2Fexample.servicebus.windows.net%2Forders&sig=',
            $request->getHeaderLine('Authorization'),
        );

        $properties = $this->brokerProperties($request);
        self::assertSame($result->id, $properties['MessageId']);
        self::assertSame('key-1', $properties['CorrelationId']);
        self::assertSame('"abc"', $request->getHeaderLine('trace'));
        self::assertSame('2', $request->getHeaderLine('attempt'));

        $message = $driver->receive('orders', new ReceiveOptions(1000));

        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame(['trace' => 'abc', 'attempt' => 2], $message->headers());
        self::assertSame('key-1', $message->key());
        self::assertSame($result->id, $message->id());
        self::assertSame(1, $message->attempts());
    }

    public function test_ack_deletes_the_locked_message(): void
    {
        $driver = $this->createDriver();
        $driver->publish('orders', new Message('ack-me'));
        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);

        $message->ack();

        $request = $this->lastRequest();
        self::assertSame('DELETE', $request->getMethod());
        self::assertSame('/orders/messages/' . $message->id() . '/lock-1', $request->getUri()->getPath());

        self::assertNull($driver->receive('orders', new ReceiveOptions(10)));
    }

    public function test_ack_on_an_already_settled_message_reports_a_lost_lock(): void
    {
        $driver = $this->createDriver();
        $driver->publish('orders', new Message('ack-me'));
        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);
        $message->ack();

        try {
            $message->ack();
            self::fail('Expected a lock-lost RequestException.');
        } catch (RequestException $exception) {
            self::assertSame(404, $exception->statusCode());
            self::assertStringContainsString('lock lost', $exception->getMessage());
        }
    }

    public function test_reject_with_requeue_unlocks_the_message(): void
    {
        $driver = $this->createDriver();
        $driver->publish('orders', new Message('retry-me'));

        $first = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($first);
        $first->reject(true);

        self::assertSame('PUT', $this->lastRequest()->getMethod());

        $second = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($second);
        self::assertSame('retry-me', $second->body());
        self::assertSame(2, $second->attempts());
        $second->ack();
    }

    public function test_reject_without_requeue_completes_the_message(): void
    {
        $driver = $this->createDriver();
        $driver->publish('orders', new Message('drop-me'));

        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);
        $message->reject(false);

        self::assertSame('DELETE', $this->lastRequest()->getMethod());
        self::assertNull($driver->receive('orders', new ReceiveOptions(10)));
    }

    public function test_receive_returns_null_on_no_content(): void
    {
        $driver = $this->createDriver();

        self::assertNull($driver->receive('orders', new ReceiveOptions(10)));
        self::assertStringContainsString('timeout=1', $this->lastRequest()->getUri()->getQuery());
    }

    public function test_receive_converts_the_timeout_to_whole_seconds(): void
    {
        $driver = $this->createDriver();

        $driver->receive('orders', new ReceiveOptions(1500));
        self::assertSame('timeout=2', $this->lastRequest()->getUri()->getQuery());

        $driver->receive('orders', new ReceiveOptions(0));
        self::assertSame('timeout=30', $this->lastRequest()->getUri()->getQuery());
    }

    public function test_delay_sets_scheduled_enqueue_time(): void
    {
        $driver = $this->createDriver();
        $before = time();

        $driver->publish('orders', new Message('later', [], null, 1500));

        $properties = $this->brokerProperties($this->history[0]['request']);
        self::assertArrayHasKey('ScheduledEnqueueTimeUtc', $properties);
        $scheduled = (int) strtotime((string) $properties['ScheduledEnqueueTimeUtc']);
        self::assertGreaterThanOrEqual($before + 1, $scheduled);
        self::assertLessThanOrEqual($before + 3, $scheduled);

        // The fake honours the schedule, so the message is not visible yet.
        self::assertNull($driver->receive('orders', new ReceiveOptions(10)));
    }

    public function test_explicit_scheduled_enqueue_time_header_is_sent(): void
    {
        $driver = $this->createDriver();
        $timestamp = strtotime('2030-01-01 00:00:00 UTC');

        $driver->publish('orders', new Message('later', [
            'ScheduledEnqueueTimeUtc' => '2030-01-01 00:00:00 UTC',
        ]));

        $properties = $this->brokerProperties($this->history[0]['request']);
        self::assertSame(gmdate('D, d M Y H:i:s \G\M\T', $timestamp), $properties['ScheduledEnqueueTimeUtc']);
    }

    public function test_delay_and_scheduled_header_cannot_be_combined(): void
    {
        $driver = $this->createDriver();

        $this->expectException(\InvalidArgumentException::class);

        $driver->publish('orders', new Message('later', [
            'ScheduledEnqueueTimeUtc' => '2030-01-01 00:00:00 UTC',
        ], null, 1000));
    }

    public function test_system_headers_map_to_broker_properties(): void
    {
        $driver = $this->createDriver();

        $driver->publish('orders', new Message('body', [
            'Label' => 'OrderCreated',
            'ReplyTo' => 'reply-queue',
            'ReplyToSessionId' => 'reply-session',
            'To' => 'orders',
            'SessionId' => 'session-1',
            'PartitionKey' => 'partition-1',
            'TimeToLive' => 60,
            'ContentType' => 'application/json',
        ]));

        $request = $this->history[0]['request'];
        $properties = $this->brokerProperties($request);

        self::assertSame('OrderCreated', $properties['Label']);
        self::assertSame('reply-queue', $properties['ReplyTo']);
        self::assertSame('reply-session', $properties['ReplyToSessionId']);
        self::assertSame('orders', $properties['To']);
        self::assertSame('session-1', $properties['SessionId']);
        self::assertSame('partition-1', $properties['PartitionKey']);
        self::assertSame(60, $properties['TimeToLive']);
        self::assertSame('application/json', $request->getHeaderLine('Content-Type'));
        self::assertSame('', $request->getHeaderLine('ContentType'));
    }

    public function test_message_id_header_overrides_the_generated_id(): void
    {
        $driver = $this->createDriver();

        $result = $driver->publish('orders', new Message('body', ['MessageId' => 'custom-id']));

        self::assertSame('custom-id', $result->id);
        self::assertSame('custom-id', $this->brokerProperties($this->history[0]['request'])['MessageId']);
    }

    public function test_custom_headers_roundtrip_with_their_types(): void
    {
        $driver = $this->createDriver();

        $driver->publish('orders', new Message('body', [
            'trace' => 'abc',
            'attempt' => 3,
            'flag' => false,
            'ratio' => 1.5,
        ]));

        $message = $driver->receive('orders', new ReceiveOptions(1000));

        self::assertNotNull($message);
        self::assertSame(['trace' => 'abc', 'attempt' => 3, 'flag' => false, 'ratio' => 1.5], $message->headers());
    }

    public function test_key_and_correlation_id_header_must_agree(): void
    {
        $driver = $this->createDriver();

        $this->expectException(\InvalidArgumentException::class);

        $driver->publish('orders', new Message('body', ['CorrelationId' => 'other'], 'key-1'));
    }

    public function test_reserved_headers_are_rejected_instead_of_dropped(): void
    {
        $driver = $this->createDriver();

        $this->expectException(\InvalidArgumentException::class);

        $driver->publish('orders', new Message('body', ['Authorization' => 'secret']));
    }

    public function test_non_scalar_headers_are_rejected(): void
    {
        $driver = $this->createDriver();

        $this->expectException(\InvalidArgumentException::class);

        $driver->publish('orders', new Message('body', ['trace' => ['nested']]));
    }

    public function test_subscription_configuration_builds_topic_and_subscription_paths(): void
    {
        $driver = $this->createDriver(['topic' => 'events', 'subscription' => 'workers']);

        $result = $driver->publish('', new Message('event'));
        self::assertSame('', $result->destination);
        self::assertSame('/events/messages', $this->lastRequest()->getUri()->getPath());

        $driver->receive('', new ReceiveOptions(10));
        self::assertSame('/events/subscriptions/workers/messages/head', $this->lastRequest()->getUri()->getPath());
    }

    public function test_explicit_subscription_destination_is_receivable_but_not_publishable(): void
    {
        $driver = $this->createDriver();

        $driver->receive('events/subscriptions/workers', new ReceiveOptions(10));
        self::assertSame(
            '/events/subscriptions/workers/messages/head',
            $this->lastRequest()->getUri()->getPath(),
        );

        $this->expectException(\InvalidArgumentException::class);

        $driver->publish('events/subscriptions/workers', new Message('nope'));
    }

    public function test_custom_entity_paths_are_validated(): void
    {
        $driver = $this->createDriver();

        $this->expectException(\InvalidArgumentException::class);

        $driver->publish('../admin', new Message('nope'));
    }

    public function test_namespace_builds_the_default_endpoint(): void
    {
        $driver = $this->createDriver([
            'endpoint' => null,
            'namespace' => 'My-Namespace',
        ]);

        $driver->publish('orders', new Message('body'));

        $uri = $this->history[0]['request']->getUri();
        self::assertSame('https', $uri->getScheme());
        self::assertSame('my-namespace.servicebus.windows.net', $uri->getHost());
    }

    public function test_endpoint_is_normalised(): void
    {
        $driver = $this->createDriver(['endpoint' => 'HTTPS://Example.ServiceBus.Windows.NET/']);

        $driver->publish('orders', new Message('body'));

        $uri = $this->history[0]['request']->getUri();
        self::assertSame('https', $uri->getScheme());
        self::assertSame('example.servicebus.windows.net', $uri->getHost());
        self::assertSame('/orders/messages', $uri->getPath());
    }

    public function test_authentication_failure_is_mapped(): void
    {
        $driver = $this->createDriver([], 'different-key');

        try {
            $driver->publish('orders', new Message('body'));
            self::fail('Expected an AuthenticationException.');
        } catch (AuthenticationException $exception) {
            self::assertStringContainsString('HTTP 401', $exception->getMessage());
        }
    }

    public function test_missing_entity_is_mapped(): void
    {
        $driver = $this->createDriver();

        try {
            $driver->publish('missing-queue', new Message('body'));
            self::fail('Expected a RequestException.');
        } catch (RequestException $exception) {
            self::assertSame(410, $exception->statusCode());
            self::assertStringContainsString('does not exist', $exception->getMessage());
        }
    }

    public function test_capabilities(): void
    {
        $capabilities = $this->createDriver()->capabilities();

        self::assertTrue($capabilities->ack);
        self::assertTrue($capabilities->requeue);
        self::assertTrue($capabilities->delay);
        self::assertFalse($capabilities->consumerGroups);
        self::assertFalse($capabilities->batchPublish);
        self::assertFalse($capabilities->ordering);
        self::assertTrue($capabilities->headers);
    }

    public function test_publish_batch_is_unsupported(): void
    {
        $driver = $this->createDriver();

        $this->expectException(UnsupportedFeatureException::class);

        $driver->publishBatch('orders', [new Message('one'), new Message('two')]);
    }

    public function test_invalid_configuration_is_rejected(): void
    {
        $cases = [
            'missing key name' => ['shared_access_key_name' => null],
            'missing key' => ['shared_access_key' => null],
            'missing queue and topic' => ['queue' => null],
            'topic without subscription' => ['queue' => null, 'topic' => 'events'],
            'queue with topic' => ['queue' => 'orders', 'topic' => 'events', 'subscription' => 'sub'],
            'invalid endpoint' => ['endpoint' => 'ftp://example.servicebus.windows.net'],
            'invalid timeout' => ['timeout' => 0],
        ];

        foreach ($cases as $label => $overrides) {
            try {
                $this->createDriver($overrides);
                self::fail('Expected configuration [' . $label . '] to be rejected.');
            } catch (\InvalidArgumentException) {
                self::assertTrue(true);
            }
        }
    }

    private function createDriver(array $overrides = [], ?string $key = null): ServiceBusDriver
    {
        $this->transport = new ServiceBusMockTransport(self::KEY_NAME, $key ?? self::KEY);

        $stack = HandlerStack::create($this->transport);
        $this->history = [];
        $stack->push(Middleware::history($this->history));

        $config = array_merge([
            'driver' => 'azure-service-bus',
            'endpoint' => self::ENDPOINT,
            'queue' => 'default',
            'shared_access_key_name' => self::KEY_NAME,
            'shared_access_key' => self::KEY,
            'timeout' => 30.0,
            'client' => new Client(['handler' => $stack]),
        ], $overrides);

        if (array_key_exists('topic', $overrides) && !array_key_exists('queue', $overrides)) {
            unset($config['queue']);
        }

        $config = array_filter($config, static fn (mixed $value): bool => $value !== null);

        $driver = (new ServiceBusFactory())->create(ConnectionConfig::fromArray('azure', $config));
        self::assertInstanceOf(ServiceBusDriver::class, $driver);

        return $driver;
    }

    /**
     * @return array<string, mixed>
     */
    private function brokerProperties(RequestInterface $request): array
    {
        $decoded = json_decode($request->getHeaderLine('BrokerProperties'), true);
        self::assertIsArray($decoded);

        /** @var array<string, mixed> $decoded */
        return $decoded;
    }

    private function lastRequest(): RequestInterface
    {
        self::assertNotSame([], $this->history);

        return $this->history[count($this->history) - 1]['request'];
    }
}
