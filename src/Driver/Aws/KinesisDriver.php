<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Aws;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\RequestException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use GuzzleHttp\ClientInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * AWS Kinesis driver using the JSON protocol and SigV4.
 *
 * Consumption is a pull loop over shard iterators. Acknowledgements advance an
 * in-memory checkpoint (per shard) and optionally call a user callback; Kinesis
 * itself has no per-message acknowledgement.
 */
final class KinesisDriver implements DriverInterface
{
    private const TARGET_PREFIX = 'Kinesis_20131202.';

    private AwsHttpClient $client;
    private LoggerInterface $logger;

    /** @var (\Closure(string, string, string): void)|null */
    private ?\Closure $checkpoint = null;

    /** @var array<string, list<string>> */
    private array $shards = [];

    /** @var array<string, array<string, string>> */
    private array $iterators = [];

    /** @var array<string, array<string, bool>> */
    private array $exhausted = [];

    /** @var array<string, list<array{shard: string, record: array<string, mixed>}>> */
    private array $buffers = [];

    /** @var array<string, array<string, string>> */
    private array $checkpoints = [];

    /** @var array<string, float> */
    private array $reinitAt = [];

    public function __construct(ConnectionConfig $config, ?LoggerInterface $logger = null, ?ClientInterface $http = null)
    {
        $this->logger = $logger ?? new NullLogger();
        $this->client = new AwsHttpClient(AwsConfig::fromConnectionConfig($config), $this->logger, $http);

        $checkpoint = $config->get('checkpoint');
        if ($checkpoint !== null) {
            if (!is_callable($checkpoint)) {
                throw new \InvalidArgumentException(sprintf(
                    'Connection [%s] option [checkpoint] must be callable.',
                    $config->name(),
                ));
            }

            $this->checkpoint = \Closure::fromCallable($checkpoint);
        }
    }

    public function publish(string $destination, Message $message, array $options = []): PublishResult
    {
        $raw = $this->client->json('kinesis', self::TARGET_PREFIX . 'PutRecord', $this->client->endpoint('kinesis'), [
            'StreamName' => self::streamName($destination),
        ] + $this->record($message), null, '1.1');

        $id = $raw['SequenceNumber'] ?? null;

        return new PublishResult($destination, is_string($id) ? $id : null, $raw);
    }

    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        if ($messages === []) {
            return [];
        }

        if (count($messages) > 500) {
            throw new \InvalidArgumentException('Kinesis PutRecords accepts at most 500 records.');
        }

        $records = [];
        foreach ($messages as $message) {
            $records[] = $this->record($message);
        }

        $raw = $this->client->json('kinesis', self::TARGET_PREFIX . 'PutRecords', $this->client->endpoint('kinesis'), [
            'StreamName' => self::streamName($destination),
            'Records' => $records,
        ], null, '1.1');

        $returned = $raw['Records'] ?? null;
        $results = [];
        foreach (array_keys($records) as $index) {
            $entry = is_array($returned) && isset($returned[$index]) && is_array($returned[$index]) ? $returned[$index] : null;
            /** @var array<string, mixed>|null $entry */
            $id = $entry !== null && isset($entry['SequenceNumber']) && is_string($entry['SequenceNumber']) ? $entry['SequenceNumber'] : null;
            if ($id === null && $entry !== null) {
                $this->logger->warning('Kinesis PutRecords entry failed', $entry);
            }

            $results[] = new PublishResult($destination, $id, $entry);
        }

        return $results;
    }

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage
    {
        $deadline = $options->timeoutMs === 0 ? null : microtime(true) + $options->timeoutMs / 1000.0;

        while (true) {
            $buffered = $this->shift($destination);
            if ($buffered !== null) {
                return $this->receivedMessage($destination, $buffered['shard'], $buffered['record']);
            }

            $this->poll($destination);

            $buffered = $this->shift($destination);
            if ($buffered !== null) {
                return $this->receivedMessage($destination, $buffered['shard'], $buffered['record']);
            }

            if ($deadline === null) {
                usleep(200_000);

                continue;
            }

            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                return null;
            }

            usleep((int) (min($remaining, 0.2) * 1_000_000));
            if (microtime(true) >= $deadline) {
                return null;
            }
        }
    }

    public function capabilities(): Capabilities
    {
        return new Capabilities(
            ack: false,
            requeue: false,
            delay: false,
            consumerGroups: false,
            batchPublish: true,
            ordering: true,
            headers: false,
        );
    }

    public function close(): void
    {
        $this->shards = [];
        $this->iterators = [];
        $this->exhausted = [];
        $this->buffers = [];
        $this->checkpoints = [];
        $this->reinitAt = [];
    }

    private function poll(string $stream): void
    {
        $active = 0;
        foreach ($this->shards($stream) as $shard) {
            if (isset($this->exhausted[$stream][$shard])) {
                continue;
            }

            $active++;
            $iterator = $this->iterator($stream, $shard);

            try {
                $data = $this->client->json('kinesis', self::TARGET_PREFIX . 'GetRecords', $this->client->endpoint('kinesis'), [
                    'ShardIterator' => $iterator,
                    'Limit' => 100,
                ], null, '1.1');
            } catch (RequestException $exception) {
                // Iterators expire after five minutes; recreate from the checkpoint.
                if (strpos((string) $exception->responseBody(), 'ExpiredIteratorException') !== false) {
                    unset($this->iterators[$stream][$shard]);

                    continue;
                }

                throw $exception;
            }

            $next = $data['NextShardIterator'] ?? null;
            if (is_string($next) && $next !== '') {
                $this->iterators[$stream][$shard] = $next;
            } else {
                unset($this->iterators[$stream][$shard]);
                $this->exhausted[$stream][$shard] = true;
            }

            $records = $data['Records'] ?? null;
            if (!is_array($records)) {
                continue;
            }

            foreach ($records as $record) {
                if (is_array($record)) {
                    /** @var array<string, mixed> $record */
                    $this->buffers[$stream][] = ['shard' => $shard, 'record' => $record];
                }
            }
        }

        if ($active === 0) {
            // All shards closed: resharding may have produced new ones. Relist at most every 5s.
            $now = microtime(true);
            if (($this->reinitAt[$stream] ?? 0.0) > $now) {
                return;
            }

            $this->reinitAt[$stream] = $now + 5.0;
            unset($this->shards[$stream], $this->iterators[$stream], $this->exhausted[$stream]);
        }
    }

    /**
     * @return list<string>
     */
    private function shards(string $stream): array
    {
        if (isset($this->shards[$stream])) {
            return $this->shards[$stream];
        }

        $data = $this->client->json('kinesis', self::TARGET_PREFIX . 'ListShards', $this->client->endpoint('kinesis'), [
            'StreamName' => $stream,
        ], null, '1.1');

        $ids = [];
        $shards = $data['Shards'] ?? null;
        if (is_array($shards)) {
            foreach ($shards as $shard) {
                if (is_array($shard) && isset($shard['ShardId']) && is_string($shard['ShardId'])) {
                    $ids[] = $shard['ShardId'];
                }
            }
        }

        if ($ids === []) {
            throw new ProtocolException(sprintf('Kinesis stream [%s] returned no shards.', $stream));
        }

        return $this->shards[$stream] = $ids;
    }

    private function iterator(string $stream, string $shard): string
    {
        $existing = $this->iterators[$stream][$shard] ?? null;
        if (is_string($existing) && $existing !== '') {
            return $existing;
        }

        $params = [
            'StreamName' => $stream,
            'ShardId' => $shard,
        ];

        $last = $this->checkpoints[$stream][$shard] ?? null;
        if ($last !== null) {
            $params['ShardIteratorType'] = 'AFTER_SEQUENCE_NUMBER';
            $params['StartingSequenceNumber'] = $last;
        } else {
            $params['ShardIteratorType'] = 'TRIM_HORIZON';
        }

        $data = $this->client->json('kinesis', self::TARGET_PREFIX . 'GetShardIterator', $this->client->endpoint('kinesis'), $params, null, '1.1');
        $iterator = $data['ShardIterator'] ?? null;
        if (!is_string($iterator) || $iterator === '') {
            throw new ProtocolException('Kinesis GetShardIterator response is missing ShardIterator.');
        }

        return $this->iterators[$stream][$shard] = $iterator;
    }

    /**
     * @return array{shard: string, record: array<string, mixed>}|null
     */
    private function shift(string $stream): ?array
    {
        if (!isset($this->buffers[$stream]) || $this->buffers[$stream] === []) {
            return null;
        }

        return array_shift($this->buffers[$stream]);
    }

    /**
     * @param array<string, mixed> $record
     */
    private function receivedMessage(string $stream, string $shard, array $record): ReceivedMessage
    {
        $encoded = $record['Data'] ?? null;
        $body = is_string($encoded) ? base64_decode($encoded, true) : false;

        $sequence = $record['SequenceNumber'] ?? null;
        $sequence = is_string($sequence) ? $sequence : null;
        $partitionKey = $record['PartitionKey'] ?? null;

        return new ReceivedMessage(
            body: $body === false ? '' : $body,
            headers: [],
            id: $sequence,
            key: is_string($partitionKey) ? $partitionKey : null,
            attempts: 1,
            ackCallback: function () use ($stream, $shard, $sequence): void {
                if ($sequence === null) {
                    return;
                }

                $this->checkpoints[$stream][$shard] = $sequence;
                $checkpoint = $this->checkpoint;
                if ($checkpoint !== null) {
                    $checkpoint($stream, $shard, $sequence);
                }
            },
            rejectCallback: null,
            raw: $record + ['ShardId' => $shard],
        );
    }

    /**
     * @return array{Data: string, PartitionKey: string}
     */
    private function record(Message $message): array
    {
        if ($message->headers !== []) {
            throw new UnsupportedFeatureException('Kinesis does not support message headers.');
        }

        if ($message->delayMs !== null) {
            throw new UnsupportedFeatureException('Kinesis does not support delayed delivery.');
        }

        $key = $message->key ?? bin2hex(random_bytes(16));
        if ($key === '' || strlen($key) > 256) {
            throw new \InvalidArgumentException('Kinesis partition key must be between 1 and 256 bytes.');
        }

        return [
            'Data' => base64_encode($message->body),
            'PartitionKey' => $key,
        ];
    }

    private static function streamName(string $destination): string
    {
        if (preg_match('~^[A-Za-z0-9_.-]{1,128}$~', $destination) !== 1) {
            throw new \InvalidArgumentException(sprintf('Invalid Kinesis stream name [%s].', $destination));
        }

        return $destination;
    }
}
