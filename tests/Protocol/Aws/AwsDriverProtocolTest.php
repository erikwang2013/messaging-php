<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Protocol\Aws;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Aws\KinesisFactory;
use Erikwang2013\Messaging\Driver\Aws\SnsFactory;
use Erikwang2013\Messaging\Driver\Aws\SqsFactory;
use Erikwang2013\Messaging\Exception\RequestException;
use PHPUnit\Framework\TestCase;

final class AwsDriverProtocolTest extends TestCase
{
    /** @var list<array{process: resource, pipes: array<int, resource>, log: string, port: int}> */
    private array $servers = [];

    /** @var list<DriverInterface> */
    private array $drivers = [];

    protected function tearDown(): void
    {
        foreach ($this->drivers as $driver) {
            $driver->close();
        }

        foreach ($this->servers as $server) {
            $this->stopServer($server);
        }

        $this->drivers = [];
        $this->servers = [];
    }

    public function test_sqs_publish_receive_and_ack_through_http(): void
    {
        $server = $this->startServer('sqs', 10);
        $driver = $this->driver('sqs', $server['port']);

        $result = $driver->publish('events', new Message('hello', ['trace' => 'abc'], 'group-1'));
        self::assertSame('sqs-msg-1', $result->id);

        $message = $driver->receive('events', new ReceiveOptions(200));
        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame('sqs-msg-1', $message->id());
        self::assertSame('group-1', $message->key());
        self::assertSame(2, $message->attempts());
        self::assertSame(['trace' => 'abc', 'blob' => "\x00\xff"], $message->headers());

        $message->ack();

        self::assertNull($driver->receive('events', new ReceiveOptions(50)));

        $events = $this->events($server['log']);
        self::assertSame(
            ['GetQueueUrl', 'SendMessage', 'ReceiveMessage', 'DeleteMessage', 'ReceiveMessage'],
            array_column($events, 'action'),
        );
        self::assertStringStartsWith('AWS4-HMAC-SHA256 Credential=test/', $events[1]['authorization']);

        $send = json_decode($events[1]['body'], true);
        self::assertSame('hello', $send['MessageBody']);
        self::assertSame('group-1', $send['MessageGroupId']);
        self::assertSame('rh-1', json_decode($events[3]['body'], true)['ReceiptHandle']);
    }

    public function test_sqs_reject_requeue_sets_visibility_zero(): void
    {
        $server = $this->startServer('sqs', 10);
        $driver = $this->driver('sqs', $server['port']);

        $message = $driver->receive('events', new ReceiveOptions(200));
        self::assertNotNull($message);
        $message->reject(true);

        $events = $this->events($server['log']);
        self::assertSame('ChangeMessageVisibility', $events[2]['action']);
        self::assertSame(0, json_decode($events[2]['body'], true)['VisibilityTimeout']);
    }

    public function test_sqs_reject_without_requeue_deletes(): void
    {
        $server = $this->startServer('sqs', 10);
        $driver = $this->driver('sqs', $server['port']);

        $message = $driver->receive('events', new ReceiveOptions(200));
        self::assertNotNull($message);
        $message->reject(false);

        $events = $this->events($server['log']);
        self::assertSame('DeleteMessage', $events[2]['action']);
    }

    public function test_sqs_receive_returns_null_on_timeout(): void
    {
        $server = $this->startServer('sqs_empty', 5);
        $driver = $this->driver('sqs', $server['port']);

        $start = microtime(true);
        self::assertNull($driver->receive('events', new ReceiveOptions(50)));
        self::assertLessThan(1.0, microtime(true) - $start);

        $events = $this->events($server['log']);
        self::assertSame(['GetQueueUrl', 'ReceiveMessage'], array_column($events, 'action'));
    }

    public function test_sqs_http_error_is_mapped(): void
    {
        $server = $this->startServer('sqs_error', 5);
        $driver = $this->driver('sqs', $server['port']);

        try {
            $driver->publish('http://127.0.0.1:' . $server['port'] . '/queue/events', new Message('hello'));
            self::fail('Expected a RequestException.');
        } catch (RequestException $exception) {
            self::assertSame(400, $exception->statusCode());
            self::assertStringContainsString('QueueDoesNotExist', $exception->getMessage());
        }

        self::assertSame('SendMessage', $this->events($server['log'])[0]['action']);
    }

    public function test_sns_publish_uses_the_query_api(): void
    {
        $server = $this->startServer('sns', 5);
        $driver = $this->driver('sns', $server['port']);

        $result = $driver->publish('arn:aws:sns:us-east-1:000000000000:events', new Message('hello', ['trace' => 'abc']));

        self::assertSame('sns-msg-1', $result->id);
        self::assertSame('sns-req-1', $result->raw['RequestId']);

        $event = $this->events($server['log'])[0];
        self::assertSame('Publish', $event['action']);
        self::assertStringStartsWith('AWS4-HMAC-SHA256 Credential=test/', $event['authorization']);

        $params = self::formParams($event['body']);
        self::assertSame('Publish', $params['Action']);
        self::assertSame('arn:aws:sns:us-east-1:000000000000:events', $params['TopicArn']);
        self::assertSame('hello', $params['Message']);
        self::assertSame('trace', $params['MessageAttributes.entry.1.Name']);
        self::assertSame('abc', $params['MessageAttributes.entry.1.Value.StringValue']);
    }

    public function test_kinesis_publish_receive_and_checkpoint(): void
    {
        $server = $this->startServer('kinesis', 10);
        $driver = $this->driver('kinesis', $server['port']);

        $result = $driver->publish('events', new Message('hello', [], 'pk-1'));
        self::assertSame('seq-pub-1', $result->id);

        $message = $driver->receive('events', new ReceiveOptions(200));
        self::assertNotNull($message);
        self::assertSame('hello', $message->body());
        self::assertSame('seq-pub-1', $message->id());
        self::assertSame('pk-1', $message->key());
        $message->ack();

        self::assertNull($driver->receive('events', new ReceiveOptions(50)));

        $events = $this->events($server['log']);
        self::assertSame(
            ['PutRecord', 'ListShards', 'GetShardIterator', 'GetRecords', 'GetRecords'],
            array_column($events, 'action'),
        );
        $put = json_decode($events[0]['body'], true);
        self::assertSame('hello', base64_decode($put['Data'], true));
        self::assertSame('pk-1', $put['PartitionKey']);
    }

    public function test_kinesis_http_error_is_mapped(): void
    {
        $server = $this->startServer('kinesis_error', 5);
        $driver = $this->driver('kinesis', $server['port']);

        $this->expectException(RequestException::class);

        $driver->publish('events', new Message('hello'));
    }

    private function driver(string $name, int $port): DriverInterface
    {
        $config = ConnectionConfig::fromArray($name, [
            'driver' => $name,
            'region' => 'us-east-1',
            'access_key' => 'test',
            'secret_key' => 'test',
            'endpoint' => 'http://127.0.0.1:' . $port,
            'timeout' => 3.0,
        ]);

        $factory = match ($name) {
            'sqs' => new SqsFactory(),
            'sns' => new SnsFactory(),
            'kinesis' => new KinesisFactory(),
        };

        $driver = $factory->create($config);
        $this->drivers[] = $driver;

        return $driver;
    }

    /**
     * @return array{process: resource, pipes: array<int, resource>, log: string, port: int}
     */
    private function startServer(string $scenario, int $maxRequests): array
    {
        $log = tempnam(sys_get_temp_dir(), 'aws-fake-');
        self::assertIsString($log);

        $process = proc_open(
            [PHP_BINARY, __DIR__ . '/fixtures/aws_fake.php', $scenario, $log, (string) $maxRequests],
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
        );
        self::assertIsResource($process);

        $line = fgets($pipes[1]);
        self::assertIsString($line, 'Fake AWS endpoint did not report its listening port.');

        $port = (int) trim($line);
        self::assertGreaterThan(0, $port);

        $server = [
            'process' => $process,
            'pipes' => $pipes,
            'log' => $log,
            'port' => $port,
        ];
        $this->servers[] = $server;

        return $server;
    }

    /**
     * @param array{process: resource, pipes: array<int, resource>, log: string, port: int} $server
     */
    private function stopServer(array $server): void
    {
        $status = proc_get_status($server['process']);
        if ($status['running']) {
            proc_terminate($server['process']);
        }

        foreach ($server['pipes'] as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }

        proc_close($server['process']);

        if (is_file($server['log'])) {
            unlink($server['log']);
        }
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function events(string $log): array
    {
        $lines = file($log, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        self::assertIsArray($lines);

        $events = [];
        foreach ($lines as $line) {
            $event = json_decode($line, true);
            self::assertIsArray($event);
            $events[] = $event;
        }

        return $events;
    }

    /**
     * @return array<string, string>
     */
    private static function formParams(string $body): array
    {
        $params = [];
        foreach (explode('&', $body) as $pair) {
            if ($pair === '') {
                continue;
            }

            $parts = explode('=', $pair, 2);
            $params[rawurldecode($parts[0])] = isset($parts[1]) ? rawurldecode($parts[1]) : '';
        }

        return $params;
    }
}
