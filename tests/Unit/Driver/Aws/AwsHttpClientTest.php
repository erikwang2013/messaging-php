<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Aws;

use Erikwang2013\Messaging\Driver\Aws\AwsConfig;
use Erikwang2013\Messaging\Driver\Aws\AwsHttpClient;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\RequestException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use GuzzleHttp\Client;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

final class AwsHttpClientTest extends TestCase
{
    /** @var list<array{request: RequestInterface}> */
    private array $history = [];

    public function test_endpoint_defaults_to_the_regional_endpoint(): void
    {
        self::assertSame(
            'https://sqs.us-east-1.amazonaws.com/',
            $this->client([])->endpoint('sqs'),
        );
        self::assertSame(
            'http://127.0.0.1:4566/',
            $this->client([], 'http://127.0.0.1:4566/')->endpoint('kinesis'),
        );
    }

    public function test_request_is_signed_with_host_date_and_authorization(): void
    {
        $client = $this->client([new Response(200, [], '{}')]);

        $client->request('sqs', 'POST', 'https://sqs.example.test/123/queue', ['X-Amz-Target' => 'AmazonSQS.SendMessage'], '{}');

        $request = $this->history[0]['request'];
        self::assertSame('sqs.example.test', $request->getHeaderLine('Host'));
        self::assertMatchesRegularExpression('/^\d{8}T\d{6}Z$/', $request->getHeaderLine('X-Amz-Date'));
        self::assertStringStartsWith(
            'AWS4-HMAC-SHA256 Credential=AKID/',
            $request->getHeaderLine('Authorization'),
        );
        self::assertStringContainsString('SignedHeaders=host;x-amz-date;x-amz-target', $request->getHeaderLine('Authorization'));
    }

    public function test_json_operation_sets_target_and_content_type(): void
    {
        $client = $this->client([new Response(200, [], '{"MessageId":"m-1"}')]);

        $data = $client->json('sqs', 'AmazonSQS.SendMessage', 'https://sqs.example.test/123/queue', ['QueueUrl' => 'q', 'MessageBody' => 'hi']);

        self::assertSame(['MessageId' => 'm-1'], $data);
        $request = $this->history[0]['request'];
        self::assertSame('AmazonSQS.SendMessage', $request->getHeaderLine('X-Amz-Target'));
        self::assertSame('application/x-amz-json-1.0', $request->getHeaderLine('Content-Type'));
        self::assertSame(
            ['QueueUrl' => 'q', 'MessageBody' => 'hi'],
            json_decode((string) $request->getBody(), true),
        );
    }

    public function test_kinesis_json_uses_version_1_1(): void
    {
        $client = $this->client([new Response(200, [], '{}')]);

        $client->json('kinesis', 'Kinesis_20131202.PutRecord', 'https://kinesis.example.test/', [], null, '1.1');

        self::assertSame('application/x-amz-json-1.1', $this->history[0]['request']->getHeaderLine('Content-Type'));
    }

    public function test_query_operation_posts_form_encoded_body(): void
    {
        $client = $this->client([new Response(200, [], '<PublishResponse/>')]);

        $xml = $client->query('sns', 'https://sns.example.test/', ['Action' => 'Publish', 'Message' => 'hello world']);

        self::assertSame('<PublishResponse/>', $xml);
        $request = $this->history[0]['request'];
        self::assertSame('application/x-www-form-urlencoded; charset=utf-8', $request->getHeaderLine('Content-Type'));
        self::assertSame('Action=Publish&Message=hello%20world', (string) $request->getBody());
    }

    public function test_json_error_is_mapped_to_request_exception(): void
    {
        $body = '{"__type":"com.amazonaws.sqs#QueueDoesNotExist","message":"The queue does not exist."}';
        $client = $this->client([new Response(400, [], $body)]);

        try {
            $client->json('sqs', 'AmazonSQS.SendMessage', 'https://sqs.example.test/queue', []);
            self::fail('Expected a RequestException.');
        } catch (RequestException $exception) {
            self::assertSame(400, $exception->statusCode());
            self::assertSame($body, $exception->responseBody());
            self::assertStringContainsString('QueueDoesNotExist', $exception->getMessage());
            self::assertStringContainsString('The queue does not exist.', $exception->getMessage());
        }
    }

    public function test_forbidden_response_is_mapped_to_authentication_exception(): void
    {
        $client = $this->client([new Response(403, [], '<ErrorResponse><Error><Code>SignatureDoesNotMatch</Code><Message>bad signature</Message></Error></ErrorResponse>')]);

        $this->expectException(AuthenticationException::class);
        $this->expectExceptionMessage('SignatureDoesNotMatch');

        $client->query('sns', 'https://sns.example.test/', ['Action' => 'Publish']);
    }

    public function test_server_error_is_mapped_to_request_exception(): void
    {
        $client = $this->client([new Response(500, [], 'internal error')]);

        try {
            $client->request('sqs', 'POST', 'https://sqs.example.test/', []);
            self::fail('Expected a RequestException.');
        } catch (RequestException $exception) {
            self::assertSame(500, $exception->statusCode());
        }
    }

    public function test_malformed_json_response_raises_protocol_exception(): void
    {
        $client = $this->client([new Response(200, [], 'not json')]);

        $this->expectException(ProtocolException::class);

        $client->json('sqs', 'AmazonSQS.SendMessage', 'https://sqs.example.test/', []);
    }

    public function test_timeout_is_mapped_to_timeout_exception(): void
    {
        $request = new Request('POST', 'https://sqs.example.test/');
        $client = $this->client([new ConnectException('cURL error 28: Operation timed out', $request, null, ['errno' => 28])]);

        $this->expectException(TimeoutException::class);

        $client->request('sqs', 'POST', 'https://sqs.example.test/', []);
    }

    public function test_transport_failure_is_mapped_to_connection_exception(): void
    {
        $request = new Request('POST', 'https://sqs.example.test/');
        $client = $this->client([new ConnectException('Connection refused', $request)]);

        $this->expectException(ConnectionException::class);

        $client->request('sqs', 'POST', 'https://sqs.example.test/', []);
    }

    public function test_empty_json_response_is_an_empty_array(): void
    {
        $client = $this->client([new Response(200, [], '')]);

        self::assertSame([], $client->json('sqs', 'AmazonSQS.DeleteMessage', 'https://sqs.example.test/', []));
    }

    /**
     * @param list<Response|\Throwable|\Closure> $responses
     */
    private function client(array $responses, ?string $endpoint = null): AwsHttpClient
    {
        $mock = new MockHandler($responses);
        $stack = HandlerStack::create($mock);
        $stack->push(Middleware::history($this->history));
        $http = new Client(['handler' => $stack]);

        return new AwsHttpClient(
            new AwsConfig('us-east-1', 'AKID', 'SECRET', null, $endpoint, 5.0),
            null,
            $http,
        );
    }
}
