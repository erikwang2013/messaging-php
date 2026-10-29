<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Protocol\Kafka;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Kafka\KafkaConfig;
use Erikwang2013\Messaging\Driver\Kafka\KafkaDriver;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class KafkaDriverProtocolTest extends TestCase
{
    /** @var resource|null */
    private $process = null;

    /** @var array<int, resource> */
    private array $pipes = [];

    private string $logFile = '';
    private int $port = 0;

    protected function tearDown(): void
    {
        if (is_resource($this->process)) {
            proc_terminate($this->process);
            foreach ($this->pipes as $pipe) {
                if (is_resource($pipe)) {
                    fclose($pipe);
                }
            }
            proc_close($this->process);
        }

        $this->process = null;
        $this->pipes = [];

        if ($this->logFile !== '' && file_exists($this->logFile)) {
            unlink($this->logFile);
        }
    }

    public function test_publish_and_receive_roundtrip(): void
    {
        $this->start('basic');
        $driver = $this->driver();

        $published = $driver->publish('orders', new Message(
            'hello kafka',
            ['trace' => 'abc', 'attempt' => 7],
            'key-1',
        ));

        self::assertMatchesRegularExpression('/^orders-[01]-\d+$/', (string) $published->id);

        $received = $driver->receive('orders', new ReceiveOptions(5000));
        self::assertNotNull($received);
        self::assertSame('hello kafka', $received->body());
        self::assertSame('key-1', $received->key());
        self::assertSame(['trace' => 'abc', 'attempt' => '7'], $received->headers());
        self::assertSame($published->id, $received->id());
        self::assertSame('orders', $received->raw()['topic']);

        $received->ack();
        self::assertNull($driver->receive('orders', new ReceiveOptions(200)));

        $driver->close();

        $apis = $this->loggedApis();
        foreach ([
            'ApiVersions',
            'Metadata',
            'Produce',
            'FindCoordinator',
            'JoinGroup',
            'SyncGroup',
            'OffsetFetch',
            'ListOffsets',
            'Fetch',
            'OffsetCommit',
            'LeaveGroup',
        ] as $api) {
            self::assertContains($api, $apis, 'Expected broker call ' . $api);
        }
    }

    public function test_batch_publish_consumes_every_message_in_input_order(): void
    {
        $this->start('basic');
        $driver = $this->driver();

        $results = $driver->publishBatch('orders', [
            new Message('one'),
            new Message('two'),
            new Message('three'),
        ]);

        self::assertCount(3, $results);
        self::assertSame(count($results), count(array_unique(array_map(static fn ($result) => $result->id, $results))));

        $receivedIds = [];
        $receivedBodies = [];
        for ($i = 0; $i < 3; $i++) {
            $message = $driver->receive('orders', new ReceiveOptions(5000));
            self::assertNotNull($message, 'missing batch message ' . $i);
            $receivedIds[] = $message->id();
            $receivedBodies[] = $message->body();
            $message->ack();
        }

        self::assertSame(['one', 'two', 'three'], $receivedBodies);
        self::assertSame(array_map(static fn ($result) => $result->id, $results), $receivedIds);

        $driver->close();
    }

    public function test_receive_honors_timeout_when_topic_is_empty(): void
    {
        $this->start('basic');
        $driver = $this->driver();

        $started = microtime(true);
        $message = $driver->receive('orders', new ReceiveOptions(150));
        $elapsed = microtime(true) - $started;

        self::assertNull($message);
        self::assertGreaterThanOrEqual(0.1, $elapsed);
        self::assertLessThan(5.0, $elapsed);

        $driver->close();
    }

    public function test_reject_requeue_seeks_back_and_reject_drop_commits(): void
    {
        $this->start('basic');
        $driver = $this->driver();

        $driver->publish('orders', new Message('retry me', [], 'retry-key'));

        $first = $driver->receive('orders', new ReceiveOptions(5000));
        self::assertNotNull($first);
        $first->reject(true);

        $second = $driver->receive('orders', new ReceiveOptions(5000));
        self::assertNotNull($second);
        self::assertSame($first->id(), $second->id());
        self::assertSame('retry me', $second->body());
        $second->reject(false);

        self::assertNull($driver->receive('orders', new ReceiveOptions(200)));

        $driver->close();
    }

    public function test_ack_commits_offset_for_the_next_consumer(): void
    {
        $this->start('basic');
        $firstDriver = $this->driver();
        $firstDriver->publish('orders', new Message('consume once', [], 'commit-key'));

        $message = $firstDriver->receive('orders', new ReceiveOptions(5000));
        self::assertNotNull($message);
        $message->ack();
        $firstDriver->close();

        $secondDriver = $this->driver();
        self::assertNull($secondDriver->receive('orders', new ReceiveOptions(300)));
        $secondDriver->close();

        $apis = $this->loggedApis();
        self::assertContains('OffsetCommit', $apis);
        self::assertSame(2, count(array_keys($apis, 'OffsetFetch', true)));
    }

    public function test_api_versions_falls_back_to_flexible_v3(): void
    {
        $this->start('force_v3');
        $driver = $this->driver();

        $published = $driver->publish('orders', new Message('v3', [], 'v3-key'));
        self::assertNotNull($published->id);

        $driver->close();

        $versions = [];
        foreach ($this->logEntries() as $entry) {
            if ($entry['api'] === 'ApiVersions') {
                $versions[] = $entry['version'];
            }
        }

        self::assertSame([0, 3], $versions);
    }

    public function test_broker_version_mismatch_throws_unsupported_feature(): void
    {
        $this->start('old_version');
        $driver = $this->driver();

        $this->expectException(UnsupportedFeatureException::class);
        $this->expectExceptionMessage('supports API 0 versions 0-2');

        $driver->publish('orders', new Message('too new'));
    }

    public function test_malformed_response_frame_throws_protocol_exception(): void
    {
        $this->start('malformed');
        $driver = $this->driver();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Invalid Kafka response frame size');

        $driver->publish('orders', new Message('malformed'));
    }

    private function start(string $mode): void
    {
        $this->logFile = (string) tempnam(sys_get_temp_dir(), 'kafka-broker-');
        $command = [
            PHP_BINARY,
            __DIR__ . '/fixtures/fake_kafka_broker.php',
            $this->logFile,
            $mode,
        ];

        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $process = proc_open($command, $descriptors, $pipes);
        self::assertIsResource($process, 'Unable to start the fake Kafka broker.');

        $this->process = $process;
        $this->pipes = $pipes;

        $line = fgets($pipes[1]);
        self::assertIsString($line, 'The fake Kafka broker did not report its port.');
        self::assertMatchesRegularExpression('/^PORT (\d+)$/', trim($line), $line);

        preg_match('/^PORT (\d+)$/', trim($line), $matches);
        $this->port = (int) $matches[1];
    }

    private function driver(): KafkaDriver
    {
        $config = KafkaConfig::fromConnection(ConnectionConfig::fromArray('fake', [
            'driver' => 'kafka',
            'brokers' => ['127.0.0.1:' . $this->port],
            'auto_offset_reset' => 'earliest',
            'timeout' => 3.0,
            'fetch_max_wait_ms' => 10,
            'session_timeout_ms' => 10000,
            'max_poll_records' => 1,
        ]));

        return new KafkaDriver($config);
    }

    /**
     * @return list<string>
     */
    private function loggedApis(): array
    {
        return array_column($this->logEntries(), 'api');
    }

    /**
     * @return list<array{api: string, version: int}>
     */
    private function logEntries(): array
    {
        if (!is_file($this->logFile)) {
            return [];
        }

        $entries = [];
        foreach (file($this->logFile, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [] as $line) {
            $entry = json_decode($line, true);
            if (is_array($entry) && isset($entry['api'], $entry['version'])) {
                $entries[] = ['api' => (string) $entry['api'], 'version' => (int) $entry['version']];
            }
        }

        return $entries;
    }
}
