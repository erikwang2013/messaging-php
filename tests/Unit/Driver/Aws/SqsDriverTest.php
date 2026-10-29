<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Aws;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Aws\SqsDriver;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\RequestException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use GuzzleHttp\Client;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

final class SqsDriverTest extends TestCase
{
    private const QUEUE_URL = 'https://sqs.example.test/123456789012/events';

    /** @var list<array{request: RequestInterface}> */
    private array $history = [];

    protected function setUp(): void
    {
        $this->history = [];
    }

    public function test_capabilities(): void
    {
        $capabilities = $this->driver()->capabilities();

        self::assertTrue($capabilities->ack);
        self::assertTrue($capabilities->requeue);
        self::assertTrue($capabilities->delay);
        self::assertFalse($capabilities->consumerGroups);
        self::assertTrue($capabilities->batchPublish);
        self::assertFalse($capabilities->ordering);
        self::assertTrue($capabilities->headers);
    }

    public function test_publish_sends_signed_json_request(): void
    {
        $driver = $this->driver([self::jsonResponse(['MessageId' => 'm-1', 'MD5OfMessageBody' => 'md5'])]);

        $result = $driver->publish(self::QUEUE_URL, new Message(
            'hello',
            ['trace' => 'abc', 'blob' => "\x00\xff"],
            'group-1',
            1500,
        ));

        self::assertSame(self::QUEUE_URL, $result->destination);
        self::assertSame('m-1', $result->id);

        $request = $this->history[0]['request'];
        self::assertSame('POST', $request->getMethod());
        self::assertSame(self::QUEUE_URL, (string) $request->getUri());
        self::assertSame('AmazonSQS.SendMessage', $request->getHeaderLine('X-Amz-Target'));
        self::assertSame('application/x-amz-json-1.0', $request->getHeaderLine('Content-Type'));
        self::assertStringStartsWith('AWS4-HMAC-SHA256 ', $request->getHeaderLine('Authorization'));

        $body = json_decode((string) $request->getBody(), true);
        self::assertSame(self::QUEUE_URL, $body['QueueUrl']);
        self::assertSame('hello', $body['MessageBody']);
        self::assertSame('group-1', $body['MessageGroupId']);
        self::assertSame(2, $body['DelaySeconds']);
        self::assertSame(
            ['DataType' => 'String', 'StringValue' => 'abc'],
            $body['MessageAttributes']['trace'],
        );
        self::assertSame(
            base64_encode("\x00\xff"),
            $body['MessageAttributes']['blob']['BinaryValue'],
        );
    }

    public function test_queue_name_is_resolved_once_and_cached(): void
    {
        $driver = $this->driver([
            self::jsonResponse(['QueueUrl' => self::QUEUE_URL]),
            self::jsonResponse(['MessageId' => 'm-1']),
            self::jsonResponse(['MessageId' => 'm-2']),
        ]);

        $driver->publish('events', new Message('one'));
        $driver->publish('events', new Message('two'));

        self::assertCount(3, $this->history);
        self::assertSame('AmazonSQS.GetQueueUrl', $this->history[0]['request']->getHeaderLine('X-Amz-Target'));
        self::assertSame(['QueueName' => 'events'], json_decode((string) $this->history[0]['request']->getBody(), true));
        self::assertSame(self::QUEUE_URL, (string) $this->history[1]['request']->getUri());
        self::assertSame(self::QUEUE_URL, (string) $this->history[2]['request']->getUri());
    }

    public function test_publish_batch_reports_success_and_failure_in_order(): void
    {
        $driver = $this->driver([self::jsonResponse([
            'Successful' => [
                ['Id' => '0', 'MessageId' => 'm-0'],
                ['Id' => '1', 'MessageId' => 'm-1'],
            ],
            'Failed' => [
                ['Id' => '2', 'SenderFault' => true, 'Code' => 'InvalidParameterValue', 'Message' => 'bad'],
            ],
        ])]);

        $results = $driver->publishBatch(self::QUEUE_URL, [
            new Message('one'),
            new Message('two', ['k' => 'v'], 'group-1'),
            new Message('three', [], null, 500),
        ]);

        self::assertSame(['m-0', 'm-1', null], array_map(static fn ($result): ?string => $result->id, $results));
        self::assertSame('InvalidParameterValue', $results[2]->raw['Code']);

        $request = $this->history[0]['request'];
        self::assertSame('AmazonSQS.SendMessageBatch', $request->getHeaderLine('X-Amz-Target'));
        $body = json_decode((string) $request->getBody(), true);
        self::assertCount(3, $body['Entries']);
        self::assertSame('group-1', $body['Entries'][1]['MessageGroupId']);
        self::assertSame(1, $body['Entries'][2]['DelaySeconds']);
    }

    public function test_batch_limits_and_empty_batches(): void
    {
        $driver = $this->driver();

        self::assertSame([], $driver->publishBatch(self::QUEUE_URL, []));

        $this->expectException(\InvalidArgumentException::class);
        $driver->publishBatch(self::QUEUE_URL, array_fill(0, 11, new Message('x')));
    }

    public function test_receive_maps_message_and_ack_deletes_it(): void
    {
        $driver = $this->driver([
            self::jsonResponse(['Messages' => [[
                'MessageId' => 'm-1',
                'ReceiptHandle' => 'rh-1',
                'Body' => 'hello',
                'MessageGroupId' => 'group-1',
                'Attributes' => ['ApproximateReceiveCount' => '3'],
                'MessageAttributes' => [
                    'trace' => ['DataType' => 'String', 'StringValue' => 'abc'],
                    'blob' => ['DataType' => 'Binary', 'BinaryValue' => base64_encode("\x00\xff")],
                ],
            ]]]),
            self::jsonResponse([]),
        ]);

        $message = $driver->receive(self::QUEUE_URL, new ReceiveOptions(100));

        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame('m-1', $message->id());
        self::assertSame('group-1', $message->key());
        self::assertSame(3, $message->attempts());
        self::assertSame(['trace' => 'abc', 'blob' => "\x00\xff"], $message->headers());

        $receive = json_decode((string) $this->history[0]['request']->getBody(), true);
        self::assertSame(0, $receive['WaitTimeSeconds']);
        self::assertSame(30, $receive['VisibilityTimeout']);
        self::assertSame(['All'], $receive['AttributeNames']);

        $message->ack();

        $delete = $this->history[1]['request'];
        self::assertSame('AmazonSQS.DeleteMessage', $delete->getHeaderLine('X-Amz-Target'));
        $deleteBody = json_decode((string) $delete->getBody(), true);
        self::assertSame(self::QUEUE_URL, $deleteBody['QueueUrl']);
        self::assertSame('rh-1', $deleteBody['ReceiptHandle']);
    }

    public function test_reject_requeue_sets_visibility_to_zero(): void
    {
        $driver = $this->driver([
            self::jsonResponse(['Messages' => [['MessageId' => 'm-1', 'ReceiptHandle' => 'rh-1', 'Body' => 'x']]]),
            self::jsonResponse([]),
        ]);

        $message = $driver->receive(self::QUEUE_URL, new ReceiveOptions(100));

        self::assertNotNull($message);
        $message->reject(true);

        $request = $this->history[1]['request'];
        self::assertSame('AmazonSQS.ChangeMessageVisibility', $request->getHeaderLine('X-Amz-Target'));
        $body = json_decode((string) $request->getBody(), true);
        self::assertSame(0, $body['VisibilityTimeout']);
        self::assertSame('rh-1', $body['ReceiptHandle']);
    }

    public function test_reject_without_requeue_deletes(): void
    {
        $driver = $this->driver([
            self::jsonResponse(['Messages' => [['MessageId' => 'm-1', 'ReceiptHandle' => 'rh-1', 'Body' => 'x']]]),
            self::jsonResponse([]),
        ]);

        $message = $driver->receive(self::QUEUE_URL, new ReceiveOptions(100));

        self::assertNotNull($message);
        $message->reject(false);

        self::assertSame('AmazonSQS.DeleteMessage', $this->history[1]['request']->getHeaderLine('X-Amz-Target'));
    }

    public function test_receive_returns_null_after_timeout(): void
    {
        $mock = new MockHandler([static fn (): Response => new Response(200, [], '{}')]);
        $stack = HandlerStack::create($mock);
        $stack->push(Middleware::history($this->history));
        $driver = $this->driver(http: new Client(['handler' => $stack]));

        $start = microtime(true);
        self::assertNull($driver->receive(self::QUEUE_URL, new ReceiveOptions(30)));
        self::assertLessThan(1.0, microtime(true) - $start);
        self::assertNotSame([], $this->history);
    }

    public function test_http_errors_are_mapped(): void
    {
        $driver = $this->driver([self::jsonResponse(
            ['__type' => 'AWS.SimpleQueueService.NonExistentQueue', 'message' => 'missing'],
            400,
        )]);

        try {
            $driver->publish(self::QUEUE_URL, new Message('x'));
            self::fail('Expected a RequestException.');
        } catch (RequestException $exception) {
            self::assertSame(400, $exception->statusCode());
            self::assertStringContainsString('NonExistentQueue', $exception->getMessage());
        }
    }

    public function test_forbidden_is_an_authentication_failure(): void
    {
        $driver = $this->driver([self::jsonResponse(['__type' => 'AccessDeniedException', 'message' => 'denied'], 403)]);

        $this->expectException(AuthenticationException::class);

        $driver->publish(self::QUEUE_URL, new Message('x'));
    }

    public function test_malformed_success_response_raises_protocol_exception(): void
    {
        $driver = $this->driver([new Response(200, [], 'nope')]);

        $this->expectException(ProtocolException::class);

        $driver->publish(self::QUEUE_URL, new Message('x'));
    }

    public function test_timeout_is_reported(): void
    {
        $request = new Request('POST', self::QUEUE_URL);
        $driver = $this->driver([new ConnectException('Operation timed out', $request, null, ['errno' => 28])]);

        $this->expectException(TimeoutException::class);

        $driver->publish(self::QUEUE_URL, new Message('x'));
    }

    public function test_invalid_bodies_are_rejected_before_any_request(): void
    {
        $driver = $this->driver();

        foreach (["\xff\xfe", ''] as $body) {
            try {
                $driver->publish(self::QUEUE_URL, new Message($body));
                self::fail('Expected an InvalidArgumentException.');
            } catch (\InvalidArgumentException) {
            }
        }

        self::assertSame([], $this->history);
    }

    public function test_delay_limits_and_fifo_delay(): void
    {
        $driver = $this->driver();

        $this->expectException(UnsupportedFeatureException::class);
        $driver->publish('https://sqs.example.test/123/events.fifo', new Message('x', [], null, 1000));
    }

    public function test_delay_above_nine_hundred_seconds_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver()->publish(self::QUEUE_URL, new Message('x', [], null, 900_001));
    }

    public function test_invalid_queue_name_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver()->publish('bad queue name', new Message('x'));
    }

    public function test_invalid_visibility_timeout_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver(options: ['visibility_timeout' => -1]);
    }

    /**
     * @param list<Response|\Throwable|callable|null> $responses
     * @param array<string, mixed> $options
     */
    private function driver(array $responses = [], array $options = [], ?Client $http = null): SqsDriver
    {
        $config = ConnectionConfig::fromArray('sqs', $options + [
            'driver' => 'sqs',
            'region' => 'us-east-1',
            'access_key' => 'AKID',
            'secret_key' => 'SECRET',
            'timeout' => 2.0,
        ]);

        if ($http === null) {
            $stack = HandlerStack::create(new MockHandler($responses));
            $stack->push(Middleware::history($this->history));
            $http = new Client(['handler' => $stack]);
        }

        return new SqsDriver($config, null, $http);
    }

    /**
     * @param array<string, mixed> $body
     */
    private static function jsonResponse(array $body, int $status = 200): Response
    {
        return new Response($status, ['Content-Type' => 'application/x-amz-json-1.0'], json_encode($body));
    }
}
