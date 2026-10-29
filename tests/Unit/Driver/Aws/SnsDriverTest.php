<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Aws;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Aws\SnsDriver;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\RequestException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use GuzzleHttp\Client;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

final class SnsDriverTest extends TestCase
{
    private const TOPIC_ARN = 'arn:aws:sns:us-east-1:000000000000:events';

    /** @var list<array{request: RequestInterface}> */
    private array $history = [];

    protected function setUp(): void
    {
        $this->history = [];
    }

    public function test_capabilities(): void
    {
        $capabilities = $this->driver()->capabilities();

        self::assertFalse($capabilities->ack);
        self::assertFalse($capabilities->requeue);
        self::assertFalse($capabilities->delay);
        self::assertFalse($capabilities->consumerGroups);
        self::assertFalse($capabilities->batchPublish);
        self::assertFalse($capabilities->ordering);
        self::assertTrue($capabilities->headers);
    }

    public function test_publish_sends_query_request_and_parses_message_id(): void
    {
        $driver = $this->driver([new Response(200, [], self::publishResponse())]);

        $result = $driver->publish(self::TOPIC_ARN, new Message(
            'hello world',
            ['trace' => 'abc', 'blob' => "\xff"],
        ));

        self::assertSame(self::TOPIC_ARN, $result->destination);
        self::assertSame('msg-1', $result->id);
        self::assertSame('req-1', $result->raw['RequestId']);

        $request = $this->history[0]['request'];
        self::assertSame('POST', $request->getMethod());
        self::assertSame('https://sns.us-east-1.amazonaws.com/', (string) $request->getUri());
        self::assertSame('application/x-www-form-urlencoded; charset=utf-8', $request->getHeaderLine('Content-Type'));
        self::assertStringStartsWith('AWS4-HMAC-SHA256 ', $request->getHeaderLine('Authorization'));

        $params = self::formParams($request);
        self::assertSame('Publish', $params['Action']);
        self::assertSame('2010-03-31', $params['Version']);
        self::assertSame(self::TOPIC_ARN, $params['TopicArn']);
        self::assertSame('hello world', $params['Message']);
        self::assertSame('trace', $params['MessageAttributes.entry.1.Name']);
        self::assertSame('String', $params['MessageAttributes.entry.1.Value.DataType']);
        self::assertSame('abc', $params['MessageAttributes.entry.1.Value.StringValue']);
        self::assertSame('blob', $params['MessageAttributes.entry.2.Name']);
        self::assertSame('Binary', $params['MessageAttributes.entry.2.Value.DataType']);
        self::assertSame(base64_encode("\xff"), $params['MessageAttributes.entry.2.Value.BinaryValue']);
    }

    public function test_publish_without_headers_omits_message_attributes(): void
    {
        $driver = $this->driver([new Response(200, [], self::publishResponse())]);

        $driver->publish(self::TOPIC_ARN, new Message('hello'));

        foreach (self::formParams($this->history[0]['request']) as $name => $value) {
            self::assertStringNotContainsString('MessageAttributes', $name);
        }
    }

    public function test_receive_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->receive(self::TOPIC_ARN, new ReceiveOptions(10));
    }

    public function test_batch_publish_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publishBatch(self::TOPIC_ARN, [new Message('x')]);
    }

    public function test_message_key_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publish(self::TOPIC_ARN, new Message('x', [], 'key'));
    }

    public function test_delay_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver()->publish(self::TOPIC_ARN, new Message('x', [], null, 100));
    }

    public function test_invalid_topic_arn_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver()->publish('events', new Message('x'));
    }

    public function test_missing_message_id_raises_protocol_exception(): void
    {
        $driver = $this->driver([new Response(200, [], '<PublishResponse></PublishResponse>')]);

        $this->expectException(ProtocolException::class);

        $driver->publish(self::TOPIC_ARN, new Message('x'));
    }

    public function test_error_response_is_mapped(): void
    {
        $driver = $this->driver([new Response(400, [], '<ErrorResponse><Error><Type>Sender</Type><Code>InvalidParameter</Code><Message>bad topic</Message></Error></ErrorResponse>')]);

        try {
            $driver->publish(self::TOPIC_ARN, new Message('x'));
            self::fail('Expected a RequestException.');
        } catch (RequestException $exception) {
            self::assertSame(400, $exception->statusCode());
            self::assertStringContainsString('InvalidParameter', $exception->getMessage());
            self::assertStringContainsString('bad topic', $exception->getMessage());
        }
    }

    public function test_forbidden_is_an_authentication_failure(): void
    {
        $driver = $this->driver([new Response(403, [], '<ErrorResponse><Error><Code>SignatureDoesNotMatch</Code><Message>bad signature</Message></Error></ErrorResponse>')]);

        $this->expectException(AuthenticationException::class);

        $driver->publish(self::TOPIC_ARN, new Message('x'));
    }

    public function test_more_than_ten_headers_is_rejected(): void
    {
        $headers = [];
        for ($i = 0; $i < 11; $i++) {
            $headers['h' . $i] = 'v';
        }

        $this->expectException(\InvalidArgumentException::class);

        $this->driver()->publish(self::TOPIC_ARN, new Message('x', $headers));
    }

    /**
     * @param list<Response|\Throwable|callable|null> $responses
     */
    private function driver(array $responses = []): SnsDriver
    {
        $config = ConnectionConfig::fromArray('sns', [
            'driver' => 'sns',
            'region' => 'us-east-1',
            'access_key' => 'AKID',
            'secret_key' => 'SECRET',
            'timeout' => 2.0,
        ]);

        $stack = HandlerStack::create(new MockHandler($responses));
        $stack->push(Middleware::history($this->history));

        return new SnsDriver($config, null, new Client(['handler' => $stack]));
    }

    /**
     * @return array<string, string>
     */
    private static function formParams(RequestInterface $request): array
    {
        $params = [];
        foreach (explode('&', (string) $request->getBody()) as $pair) {
            if ($pair === '') {
                continue;
            }

            $parts = explode('=', $pair, 2);
            $params[rawurldecode($parts[0])] = isset($parts[1]) ? rawurldecode($parts[1]) : '';
        }

        return $params;
    }

    private static function publishResponse(): string
    {
        return '<PublishResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">'
            . '<PublishResult><MessageId>msg-1</MessageId></PublishResult>'
            . '<ResponseMetadata><RequestId>req-1</RequestId></ResponseMetadata>'
            . '</PublishResponse>';
    }
}
