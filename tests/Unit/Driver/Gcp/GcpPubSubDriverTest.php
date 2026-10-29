<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Gcp;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Gcp\GcpPubSubDriver;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\RequestException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use GuzzleHttp\Client;
use GuzzleHttp\ClientInterface;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;

final class GcpPubSubDriverTest extends TestCase
{
    /** @var list<array{request: \Psr\Http\Message\RequestInterface, response: mixed, options: array<string, mixed>}> */
    private array $history = [];

    public function test_publish_encodes_body_attributes_and_ordering_key(): void
    {
        $driver = $this->emulatorDriver([self::jsonResponse(['messageIds' => ['42']])]);

        $result = $driver->publish('orders', new Message("\x00hello", ['trace' => 'abc', 'attempt' => 2], 'key-1'));

        self::assertSame('orders', $result->destination);
        self::assertSame('42', $result->id);
        self::assertCount(1, $this->history);

        $request = $this->history[0]['request'];
        self::assertSame('POST', $request->getMethod());
        self::assertSame(
            'http://127.0.0.1:8085/v1/projects/test-project/topics/orders:publish',
            (string) $request->getUri(),
        );
        self::assertFalse($request->hasHeader('Authorization'));

        $body = $this->requestJson(0);
        self::assertSame(base64_encode("\x00hello"), $body['messages'][0]['data']);
        self::assertSame(['trace' => 'abc', 'attempt' => '2'], $body['messages'][0]['attributes']);
        self::assertSame('key-1', $body['messages'][0]['orderingKey']);
    }

    public function test_publish_omits_empty_attributes_and_key(): void
    {
        $driver = $this->emulatorDriver([self::jsonResponse(['messageIds' => ['42']])]);

        $driver->publish('orders', new Message('hello'));

        $message = $this->requestJson(0)['messages'][0];
        self::assertSame(['data' => base64_encode('hello')], $message);
    }

    public function test_batch_publish_sends_one_request_with_all_messages(): void
    {
        $driver = $this->emulatorDriver([self::jsonResponse(['messageIds' => ['1', '2']])]);

        $results = $driver->publishBatch('orders', [
            new Message('one'),
            new Message('two'),
        ]);

        self::assertCount(2, $results);
        self::assertSame('1', $results[0]->id);
        self::assertSame('2', $results[1]->id);
        self::assertCount(1, $this->history);

        $messages = $this->requestJson(0)['messages'];
        self::assertCount(2, $messages);
        self::assertSame(base64_encode('one'), $messages[0]['data']);
        self::assertSame(base64_encode('two'), $messages[1]['data']);
    }

    public function test_empty_batch_is_rejected(): void
    {
        $driver = $this->emulatorDriver([]);

        $this->expectException(\InvalidArgumentException::class);

        $driver->publishBatch('orders', []);
    }

    public function test_publish_rejects_delay(): void
    {
        $driver = $this->emulatorDriver([]);

        $this->expectException(UnsupportedFeatureException::class);

        $driver->publish('orders', new Message('hello', [], null, 100));
    }

    public function test_publish_rejects_non_scalar_headers_instead_of_dropping_them(): void
    {
        $driver = $this->emulatorDriver([]);

        $this->expectException(UnsupportedFeatureException::class);

        $driver->publish('orders', new Message('hello', ['nested' => ['a' => 1]]));
    }

    public function test_receive_parses_message_and_acknowledges_it(): void
    {
        $driver = $this->emulatorDriver([
            self::jsonResponse([
                'receivedMessages' => [[
                    'ackId' => 'ack-1',
                    'message' => [
                        'data' => base64_encode('hello'),
                        'attributes' => ['trace' => 'abc'],
                        'orderingKey' => 'key-1',
                        'messageId' => '42',
                    ],
                ]],
            ]),
            self::jsonResponse([]),
            self::jsonResponse([]),
        ]);

        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame(['trace' => 'abc'], $message->headers());
        self::assertSame('key-1', $message->key());
        self::assertSame('42', $message->id());
        self::assertSame(1, $message->attempts());
        self::assertIsArray($message->raw());

        $pullBody = $this->requestJson(0);
        self::assertSame(1, $pullBody['maxMessages']);
        self::assertFalse($pullBody['returnImmediately']);

        // Pulling a message extends its lease before it is handed to the caller.
        self::assertCount(2, $this->history);
        self::assertSame(
            'http://127.0.0.1:8085/v1/projects/test-project/subscriptions/orders:modifyAckDeadline',
            (string) $this->history[1]['request']->getUri(),
        );
        self::assertSame(
            ['ackIds' => ['ack-1'], 'ackDeadlineSeconds' => 60],
            $this->requestJson(1),
        );

        $message->ack();

        self::assertCount(3, $this->history);
        self::assertSame(
            'http://127.0.0.1:8085/v1/projects/test-project/subscriptions/orders:acknowledge',
            (string) $this->history[2]['request']->getUri(),
        );
        self::assertSame(['ackIds' => ['ack-1']], $this->requestJson(2));
    }

    public function test_pull_extends_the_ack_deadline_with_the_configured_seconds(): void
    {
        $driver = $this->emulatorDriver(
            [self::pullResponse('ack-1', 'lease-me'), self::jsonResponse([])],
            ['ack_deadline_seconds' => 45],
        );

        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);

        self::assertCount(2, $this->history);
        self::assertSame(
            'http://127.0.0.1:8085/v1/projects/test-project/subscriptions/orders:modifyAckDeadline',
            (string) $this->history[1]['request']->getUri(),
        );
        self::assertSame(
            ['ackIds' => ['ack-1'], 'ackDeadlineSeconds' => 45],
            $this->requestJson(1),
        );
    }

    public function test_zero_ack_deadline_skips_the_extension_call(): void
    {
        $driver = $this->emulatorDriver(
            [self::pullResponse('ack-1', 'no-lease')],
            ['ack_deadline_seconds' => 0],
        );

        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);

        self::assertCount(1, $this->history);
    }

    public function test_reject_with_requeue_resets_the_ack_deadline(): void
    {
        $driver = $this->emulatorDriver([
            self::pullResponse('ack-1', 'retry-me'),
            self::jsonResponse([]),
            self::jsonResponse([]),
        ]);

        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);
        $message->reject(true);

        self::assertSame(
            'http://127.0.0.1:8085/v1/projects/test-project/subscriptions/orders:modifyAckDeadline',
            (string) $this->history[2]['request']->getUri(),
        );
        self::assertSame(
            ['ackIds' => ['ack-1'], 'ackDeadlineSeconds' => 0],
            $this->requestJson(2),
        );
    }

    public function test_reject_without_requeue_acknowledges_the_message(): void
    {
        $driver = $this->emulatorDriver([
            self::pullResponse('ack-1', 'drop-me'),
            self::jsonResponse([]),
            self::jsonResponse([]),
        ]);

        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);
        $message->reject(false);

        self::assertSame(
            'http://127.0.0.1:8085/v1/projects/test-project/subscriptions/orders:acknowledge',
            (string) $this->history[2]['request']->getUri(),
        );
    }

    public function test_receive_returns_null_when_the_request_times_out(): void
    {
        $driver = $this->emulatorDriver([
            new ConnectException(
                'cURL error 28: Operation timed out',
                new Request('POST', 'http://127.0.0.1:8085/'),
                null,
                ['errno' => 28],
            ),
        ]);

        self::assertNull($driver->receive('orders', new ReceiveOptions(50)));
    }

    public function test_receive_returns_null_when_no_messages_arrive_before_the_deadline(): void
    {
        $driver = $this->emulatorDriver([self::jsonResponse([])]);

        self::assertNull($driver->receive('orders', new ReceiveOptions(5)));
    }

    public function test_receive_polls_again_within_the_timeout_after_an_empty_response(): void
    {
        $driver = $this->emulatorDriver([
            self::jsonResponse([]),
            self::pullResponse('ack-1', 'eventual'),
            self::jsonResponse([]),
        ]);

        $message = $driver->receive('orders', new ReceiveOptions(1000));
        self::assertNotNull($message);
        self::assertSame('eventual', $message->body());
        self::assertCount(3, $this->history);
    }

    public function test_receive_uses_the_configured_default_subscription(): void
    {
        $driver = $this->emulatorDriver(
            [self::pullResponse('ack-1', 'hello'), self::jsonResponse([])],
            ['subscription' => 'default-sub'],
        );

        $message = $driver->receive('', new ReceiveOptions(1000));
        self::assertNotNull($message);
        self::assertSame('hello', $message->body());

        self::assertSame(
            'http://127.0.0.1:8085/v1/projects/test-project/subscriptions/default-sub:pull',
            (string) $this->history[0]['request']->getUri(),
        );
    }

    public function test_receive_without_a_subscription_name_is_rejected(): void
    {
        $driver = $this->emulatorDriver([]);

        $this->expectException(\InvalidArgumentException::class);

        $driver->receive('', new ReceiveOptions(10));
    }

    public function test_malformed_pull_payload_is_a_protocol_error(): void
    {
        $driver = $this->emulatorDriver([
            self::jsonResponse(['receivedMessages' => [['ackId' => 'ack-1', 'message' => ['data' => '!!!']]]]),
        ]);

        $this->expectException(ProtocolException::class);

        $driver->receive('orders', new ReceiveOptions(1000));
    }

    public function test_malformed_json_response_is_a_protocol_error(): void
    {
        $driver = $this->emulatorDriver([new Response(200, [], 'not-json')]);

        $this->expectException(ProtocolException::class);

        $driver->publish('orders', new Message('hello'));
    }

    public function test_not_found_is_a_request_error_with_the_status_and_body(): void
    {
        $raw = '{"error":{"code":404,"message":"Subscription does not exist.","status":"NOT_FOUND"}}';
        $driver = $this->emulatorDriver([new Response(404, [], $raw)]);

        try {
            $driver->publish('orders', new Message('hello'));
            self::fail('Expected a RequestException.');
        } catch (RequestException $exception) {
            self::assertSame(404, $exception->statusCode());
            self::assertSame($raw, $exception->responseBody());
            self::assertStringContainsString('Subscription does not exist.', $exception->getMessage());
        }
    }

    public function test_connection_failure_is_a_connection_error(): void
    {
        $driver = $this->emulatorDriver([
            new ConnectException('Connection refused', new Request('POST', 'http://127.0.0.1:8085/'), null, ['errno' => 7]),
        ]);

        $this->expectException(ConnectionException::class);

        $driver->publish('orders', new Message('hello'));
    }

    public function test_unauthorized_is_an_authentication_error(): void
    {
        $driver = $this->emulatorDriver([new Response(401, [], '{"error":{"message":"Unauthorized"}}')]);

        $this->expectException(AuthenticationException::class);

        $driver->publish('orders', new Message('hello'));
    }

    public function test_forbidden_is_an_authentication_error(): void
    {
        $driver = $this->emulatorDriver([new Response(403, [], '{"error":{"message":"Permission denied"}}')]);

        $this->expectException(AuthenticationException::class);

        $driver->publish('orders', new Message('hello'));
    }

    public function test_publish_uses_a_cached_bearer_token(): void
    {
        $driver = $this->authenticatedDriver([
            self::tokenResponse('ya29.test'),
            self::jsonResponse(['messageIds' => ['1']]),
            self::jsonResponse(['messageIds' => ['2']]),
        ]);

        $driver->publish('orders', new Message('one'));
        $driver->publish('orders', new Message('two'));

        self::assertCount(3, $this->history);
        self::assertSame('https://oauth2.googleapis.com/token', (string) $this->history[0]['request']->getUri());

        $form = [];
        parse_str((string) $this->history[0]['request']->getBody(), $form);
        self::assertSame('urn:ietf:params:oauth:grant-type:jwt-bearer', $form['grant_type']);
        self::assertArrayHasKey('assertion', $form);

        self::assertSame('Bearer ya29.test', $this->history[1]['request']->getHeaderLine('Authorization'));
        self::assertSame('Bearer ya29.test', $this->history[2]['request']->getHeaderLine('Authorization'));
    }

    public function test_token_failure_is_an_authentication_error(): void
    {
        $driver = $this->authenticatedDriver([
            new Response(400, [], '{"error":"invalid_grant","error_description":"Invalid JWT Signature."}'),
        ]);

        $this->expectException(AuthenticationException::class);

        $driver->publish('orders', new Message('hello'));
    }

    public function test_capabilities(): void
    {
        $capabilities = $this->emulatorDriver([])->capabilities();

        self::assertTrue($capabilities->ack);
        self::assertTrue($capabilities->requeue);
        self::assertFalse($capabilities->delay);
        self::assertTrue($capabilities->consumerGroups);
        self::assertTrue($capabilities->batchPublish);
        self::assertTrue($capabilities->ordering);
        self::assertTrue($capabilities->headers);
        self::assertFalse($capabilities->requiresActiveSubscription);
    }

    /**
     * @param list<mixed> $responses
     * @param array<string, mixed> $options
     */
    private function emulatorDriver(array $responses, array $options = []): GcpPubSubDriver
    {
        return new GcpPubSubDriver(
            ConnectionConfig::fromArray('gcp', [
                'driver' => 'gcp-pubsub',
                'project_id' => 'test-project',
                'emulator_host' => '127.0.0.1:8085',
            ] + $options),
            null,
            $this->client($responses),
        );
    }

    /**
     * @param list<mixed> $responses
     * @param array<string, mixed> $options
     */
    private function authenticatedDriver(array $responses, array $options = []): GcpPubSubDriver
    {
        return new GcpPubSubDriver(
            ConnectionConfig::fromArray('gcp', [
                'driver' => 'gcp-pubsub',
                'project_id' => 'test-project',
                'credentials_json' => GcpTestCredentials::json(),
            ] + $options),
            null,
            $this->client($responses),
        );
    }

    /**
     * @param list<mixed> $responses
     */
    private function client(array $responses): ClientInterface
    {
        $container = [];
        $stack = HandlerStack::create(new MockHandler($responses));
        $stack->push(Middleware::history($container));
        $this->history = &$container;

        return new Client(['handler' => $stack, 'http_errors' => false]);
    }

    /**
     * @param array<string, mixed> $payload
     */
    private static function jsonResponse(array $payload, int $status = 200): Response
    {
        return new Response($status, ['Content-Type' => 'application/json'], json_encode($payload, JSON_THROW_ON_ERROR));
    }

    private static function pullResponse(string $ackId, string $body): Response
    {
        return self::jsonResponse([
            'receivedMessages' => [[
                'ackId' => $ackId,
                'message' => ['data' => base64_encode($body), 'messageId' => 'm-' . $ackId],
            ]],
        ]);
    }

    private static function tokenResponse(string $token): Response
    {
        return self::jsonResponse([
            'access_token' => $token,
            'expires_in' => 3600,
            'token_type' => 'Bearer',
        ]);
    }

    /**
     * @return array<string, mixed>
     */
    private function requestJson(int $index): array
    {
        $data = json_decode((string) $this->history[$index]['request']->getBody(), true);
        self::assertIsArray($data);

        return $data;
    }
}
