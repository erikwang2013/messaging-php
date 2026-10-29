<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Aws;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Aws\KinesisDriver;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\RequestException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use GuzzleHttp\Client;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

final class KinesisDriverTest extends TestCase
{
    /** @var list<array{request: RequestInterface}> */
    private array $history = [];

    /** @var list<array{0: string, 1: string, 2: string}> */
    private array $checkpoints = [];

    protected function setUp(): void
    {
        $this->history = [];
        $this->checkpoints = [];
    }

    public function test_capabilities(): void
    {
        $capabilities = $this->driver()->capabilities();

        self::assertFalse($capabilities->ack);
        self::assertFalse($capabilities->requeue);
        self::assertFalse($capabilities->delay);
        self::assertFalse($capabilities->consumerGroups);
        self::assertTrue($capabilities->batchPublish);
        self::assertTrue($capabilities->ordering);
        self::assertFalse($capabilities->headers);
    }

    public function test_publish_sends_put_record(): void
    {
        $driver = $this->driver([self::jsonResponse(['SequenceNumber' => 'seq-1', 'ShardId' => 'shard-0'])]);

        $result = $driver->publish('events', new Message('hello', [], 'pk-1'));

        self::assertSame('events', $result->destination);
        self::assertSame('seq-1', $result->id);

        $request = $this->history[0]['request'];
        self::assertSame('Kinesis_20131202.PutRecord', $request->getHeaderLine('X-Amz-Target'));
        self::assertSame('application/x-amz-json-1.1', $request->getHeaderLine('Content-Type'));

        $body = json_decode((string) $request->getBody(), true);
        self::assertSame('events', $body['StreamName']);
        self::assertSame('hello', base64_decode($body['Data'], true));
        self::assertSame('pk-1', $body['PartitionKey']);
    }

    public function test_publish_generates_a_partition_key_when_missing(): void
    {
        $driver = $this->driver([self::jsonResponse(['SequenceNumber' => 'seq-1'])]);

        $driver->publish('events', new Message('hello'));

        $body = json_decode((string) $this->history[0]['request']->getBody(), true);
        self::assertMatchesRegularExpression('/^[0-9a-f]{32}$/', $body['PartitionKey']);
    }

    public function test_publish_batch_maps_partial_failures(): void
    {
        $driver = $this->driver([self::jsonResponse(['Records' => [
            ['SequenceNumber' => 'seq-0', 'ShardId' => 'shard-0'],
            ['ErrorCode' => 'ProvisionedThroughputExceededException', 'ErrorMessage' => 'slow down'],
        ]])]);

        $results = $driver->publishBatch('events', [new Message('one', [], 'pk'), new Message('two', [], 'pk')]);

        self::assertSame(['seq-0', null], array_map(static fn ($result): ?string => $result->id, $results));
        self::assertSame('ProvisionedThroughputExceededException', $results[1]->raw['ErrorCode']);

        $body = json_decode((string) $this->history[0]['request']->getBody(), true);
        self::assertSame('Kinesis_20131202.PutRecords', $this->history[0]['request']->getHeaderLine('X-Amz-Target'));
        self::assertCount(2, $body['Records']);
    }

    public function test_batch_limits_and_empty_batches(): void
    {
        $driver = $this->driver();

        self::assertSame([], $driver->publishBatch('events', []));

        $this->expectException(\InvalidArgumentException::class);
        $driver->publishBatch('events', array_fill(0, 501, new Message('x')));
    }

    public function test_receive_pulls_records_and_ack_calls_checkpoint(): void
    {
        $driver = $this->driver([
            self::jsonResponse(['Shards' => [['ShardId' => 'shard-0']]]),
            self::jsonResponse(['ShardIterator' => 'it-1']),
            self::jsonResponse([
                'Records' => [[
                    'SequenceNumber' => 'seq-1',
                    'Data' => base64_encode("\x00\xffhello"),
                    'PartitionKey' => 'pk-1',
                ]],
                'NextShardIterator' => 'it-2',
            ]),
        ], ['checkpoint' => function (string $stream, string $shard, string $sequence): void {
            $this->checkpoints[] = [$stream, $shard, $sequence];
        }]);

        $message = $driver->receive('events', new ReceiveOptions(200));

        self::assertNotNull($message);
        self::assertSame("\x00\xffhello", $message->body());
        self::assertSame('seq-1', $message->id());
        self::assertSame('pk-1', $message->key());
        self::assertSame(1, $message->attempts());
        self::assertSame([], $message->headers());
        self::assertSame('shard-0', $message->raw()['ShardId']);

        $iteratorRequest = json_decode((string) $this->history[1]['request']->getBody(), true);
        self::assertSame('TRIM_HORIZON', $iteratorRequest['ShardIteratorType']);

        $message->ack();

        self::assertSame([['events', 'shard-0', 'seq-1']], $this->checkpoints);
    }

    public function test_receive_returns_null_after_timeout(): void
    {
        $http = $this->handlerClient(static function (RequestInterface $request): Response {
            $target = $request->getHeaderLine('X-Amz-Target');
            if (str_ends_with($target, 'ListShards')) {
                return self::jsonResponse(['Shards' => [['ShardId' => 'shard-0']]]);
            }
            if (str_ends_with($target, 'GetShardIterator')) {
                return self::jsonResponse(['ShardIterator' => 'it-1']);
            }
            if (str_ends_with($target, 'GetRecords')) {
                return self::jsonResponse(['Records' => [], 'NextShardIterator' => 'it-2']);
            }

            throw new \RuntimeException('Unexpected target ' . $target);
        });

        $driver = $this->driver([], [], $http);

        $start = microtime(true);
        self::assertNull($driver->receive('events', new ReceiveOptions(30)));
        self::assertLessThan(1.0, microtime(true) - $start);
        self::assertCount(3, $this->history);
    }

    public function test_expired_iterator_is_recreated(): void
    {
        $driver = $this->driver([
            self::jsonResponse(['Shards' => [['ShardId' => 'shard-0']]]),
            self::jsonResponse(['ShardIterator' => 'it-1']),
            new RequestException('Iterator expired', 400, '<Error><Code>ExpiredIteratorException</Code></Error>'),
            self::jsonResponse(['ShardIterator' => 'it-2']),
            self::jsonResponse([
                'Records' => [['SequenceNumber' => 'seq-1', 'Data' => base64_encode('hello'), 'PartitionKey' => 'pk']],
                'NextShardIterator' => 'it-3',
            ]),
        ]);

        $message = $driver->receive('events', new ReceiveOptions(500));

        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertCount(5, $this->history);
        self::assertSame('Kinesis_20131202.GetShardIterator', $this->history[3]['request']->getHeaderLine('X-Amz-Target'));
    }

    public function test_stream_without_shards_raises_protocol_exception(): void
    {
        $driver = $this->driver([self::jsonResponse(['Shards' => []])]);

        $this->expectException(ProtocolException::class);

        $driver->receive('events', new ReceiveOptions(10));
    }

    public function test_reject_is_unsupported(): void
    {
        $driver = $this->driver([
            self::jsonResponse(['Shards' => [['ShardId' => 'shard-0']]]),
            self::jsonResponse(['ShardIterator' => 'it-1']),
            self::jsonResponse(['Records' => [['SequenceNumber' => 'seq-1', 'Data' => base64_encode('x'), 'PartitionKey' => 'pk']], 'NextShardIterator' => 'it-2']),
        ]);

        $message = $driver->receive('events', new ReceiveOptions(200));

        self::assertNotNull($message);
        $this->expectException(UnsupportedFeatureException::class);
        $message->reject(true);
    }

    public function test_headers_and_delay_are_unsupported(): void
    {
        $driver = $this->driver();

        try {
            $driver->publish('events', new Message('x', ['trace' => 'abc']));
            self::fail('Expected an UnsupportedFeatureException.');
        } catch (UnsupportedFeatureException) {
        }

        $this->expectException(UnsupportedFeatureException::class);
        $driver->publish('events', new Message('x', [], null, 100));
    }

    public function test_invalid_stream_name_and_partition_key_are_rejected(): void
    {
        $driver = $this->driver();

        try {
            $driver->publish('bad stream', new Message('x'));
            self::fail('Expected an InvalidArgumentException.');
        } catch (\InvalidArgumentException) {
        }

        $this->expectException(\InvalidArgumentException::class);
        $driver->publish('events', new Message('x', [], ''));
    }

    public function test_checkpoint_option_must_be_callable(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->driver([], ['checkpoint' => 'not-callable']);
    }

    /**
     * @param list<Response|\Throwable|callable|null> $responses
     * @param array<string, mixed> $options
     */
    private function driver(array $responses = [], array $options = [], ?Client $http = null): KinesisDriver
    {
        $config = ConnectionConfig::fromArray('kinesis', $options + [
            'driver' => 'kinesis',
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

        return new KinesisDriver($config, null, $http);
    }

    private function handlerClient(callable $handler): Client
    {
        $stack = HandlerStack::create(static function (RequestInterface $request, array $options) use ($handler) {
            return Create::promiseFor($handler($request, $options));
        });
        $stack->push(Middleware::history($this->history));

        return new Client(['handler' => $stack]);
    }

    /**
     * @param array<string, mixed> $body
     */
    private static function jsonResponse(array $body, int $status = 200): Response
    {
        return new Response($status, ['Content-Type' => 'application/x-amz-json-1.1'], json_encode($body));
    }
}
