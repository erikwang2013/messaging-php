<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Psr\Log\LoggerInterface;

final class KafkaDriver implements DriverInterface
{
    private KafkaCluster $cluster;
    private ?KafkaConsumer $consumer = null;

    /** @var array<string, int> */
    private array $roundRobin = [];

    public function __construct(
        private KafkaConfig $config,
        private ?LoggerInterface $logger = null,
    ) {
        $this->cluster = new KafkaCluster($config, $logger);
    }

    public function publish(string $destination, Message $message, array $options = []): PublishResult
    {
        $this->assertNoDelay([$message]);

        $partitions = array_keys($this->cluster->partitionsFor($destination));
        $partition = $message->key !== null
            ? Murmur2::partition($message->key, count($partitions))
            : $this->roundRobinPartition($destination, $partitions);

        $batch = RecordBatch::encode([[
            'key' => $message->key,
            'value' => $message->body,
            'headers' => self::normalizeHeaders($message->headers),
            'timestamp' => self::nowMs(),
        ]]);

        $result = $this->producePartitions($destination, [$partition => $batch], 1);
        $offset = $this->offsetFor($result, $destination, $partition, 0);

        return new PublishResult(
            $destination,
            sprintf('%s-%d-%d', $destination, $partition, $offset),
            ['topic' => $destination, 'partition' => $partition, 'offset' => $offset],
        );
    }

    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        $this->assertNoDelay($messages);

        if ($messages === []) {
            return [];
        }

        $partitions = array_keys($this->cluster->partitionsFor($destination));
        $timestamp = self::nowMs();
        $byPartition = [];
        $placement = [];

        foreach ($messages as $message) {
            if (!$message instanceof Message) {
                throw new \InvalidArgumentException('Kafka publishBatch expects a list of Message objects.');
            }

            $partition = $message->key !== null
                ? Murmur2::partition($message->key, count($partitions))
                : $this->roundRobinPartition($destination, $partitions);

            $index = count($byPartition[$partition] ?? []);
            $byPartition[$partition][] = [
                'key' => $message->key,
                'value' => $message->body,
                'headers' => self::normalizeHeaders($message->headers),
                'timestamp' => $timestamp,
            ];
            $placement[] = [$partition, $index];
        }

        $batches = [];
        foreach ($byPartition as $partition => $records) {
            $batches[$partition] = RecordBatch::encode($records);
        }

        $result = $this->producePartitions($destination, $batches, count($messages));

        $baseOffsets = [];
        foreach (array_keys($batches) as $partition) {
            $baseOffsets[$partition] = $this->offsetFor($result, $destination, $partition, 0);
        }

        $results = [];
        foreach ($placement as [$partition, $index]) {
            $offset = $baseOffsets[$partition] + $index;
            $results[] = new PublishResult(
                $destination,
                sprintf('%s-%d-%d', $destination, $partition, $offset),
                ['topic' => $destination, 'partition' => $partition, 'offset' => $offset],
            );
        }

        return $results;
    }

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage
    {
        $this->consumer ??= new KafkaConsumer($this->cluster, $this->config, $this->logger);

        return $this->consumer->receive($destination, $options->timeoutMs);
    }

    public function capabilities(): Capabilities
    {
        return new Capabilities(
            ack: true,
            requeue: true,
            delay: false,
            consumerGroups: true,
            batchPublish: true,
            ordering: true,
            headers: true,
            requiresActiveSubscription: false,
        );
    }

    public function close(): void
    {
        $this->consumer?->close();
        $this->consumer = null;
        $this->cluster->close();
    }

    /**
     * @param list<Message> $messages
     */
    private function assertNoDelay(array $messages): void
    {
        foreach ($messages as $message) {
            if ($message->delayMs !== null) {
                throw new UnsupportedFeatureException('Kafka does not support per-message publish delays.');
            }
        }
    }

    /**
     * @param list<int> $partitions
     */
    private function roundRobinPartition(string $topic, array $partitions): int
    {
        $count = count($partitions);
        $position = $this->roundRobin[$topic] ?? 0;
        $partition = $partitions[$position % $count];
        $this->roundRobin[$topic] = ($position + 1) % $count;

        return $partition;
    }

    /**
     * @param array<int, string> $batches partition index => record batch
     * @return array<string, array<int, array{error: int, base_offset: int}>>
     */
    private function producePartitions(string $topic, array $batches, int $recordCount): array
    {
        $byLeader = [];
        foreach ($batches as $partition => $batch) {
            $byLeader[$this->cluster->leaderId($topic, $partition)][$partition] = $batch;
        }

        $result = [];
        $timeoutMs = max(1, (int) ceil($this->config->timeout * 1000));

        foreach ($byLeader as $leaderId => $partitionBatches) {
            $response = $this->cluster->brokerById($leaderId)->produce(
                $topic,
                $partitionBatches,
                $this->config->acks,
                $timeoutMs,
            );

            foreach ($response[$topic] ?? [] as $partition => $partitionResult) {
                if (KafkaErrors::isRetriable($partitionResult['error'])) {
                    // No automatic retry, but let the caller's retry resolve a new leader.
                    try {
                        $this->cluster->refreshMetadata([$topic]);
                    } catch (\Throwable $exception) {
                        $this->logger?->debug('Kafka metadata refresh failed during produce.', [
                            'topic' => $topic,
                            'error' => $exception->getMessage(),
                        ]);
                    }
                }

                KafkaErrors::assertNone(
                    $partitionResult['error'],
                    sprintf('Produce for [%s-%d]', $topic, $partition),
                );
                $result[$topic][$partition] = $partitionResult;
            }
        }

        foreach ($batches as $partition => $batch) {
            if (!isset($result[$topic][$partition])) {
                throw new ProtocolException(sprintf(
                    'Kafka Produce response is missing [%s-%d] (%d records).',
                    $topic,
                    $partition,
                    $recordCount,
                ));
            }
        }

        return $result;
    }

    /**
     * @param array<string, array<int, array{error: int, base_offset: int}>> $result
     */
    private function offsetFor(array $result, string $topic, int $partition, int $index): int
    {
        $partitionResult = $result[$topic][$partition] ?? null;
        if ($partitionResult === null) {
            throw new ProtocolException(sprintf('Kafka Produce response is missing [%s-%d].', $topic, $partition));
        }

        return $partitionResult['base_offset'] + $index;
    }

    /**
     * @param array<string, mixed> $headers
     * @return array<string, ?string>
     */
    private static function normalizeHeaders(array $headers): array
    {
        $normalized = [];
        foreach ($headers as $name => $value) {
            $name = (string) $name;

            if ($value === null || is_string($value)) {
                $normalized[$name] = $value;
                continue;
            }

            if (is_bool($value)) {
                $normalized[$name] = $value ? 'true' : 'false';
                continue;
            }

            if (is_int($value) || is_float($value)) {
                $normalized[$name] = (string) $value;
                continue;
            }

            if (is_array($value) || is_object($value)) {
                $normalized[$name] = (string) json_encode($value, JSON_THROW_ON_ERROR);
                continue;
            }

            throw new UnsupportedFeatureException(sprintf(
                'Kafka header [%s] has an unsupported value type [%s].',
                $name,
                get_debug_type($value),
            ));
        }

        return $normalized;
    }

    private static function nowMs(): int
    {
        return (int) floor(microtime(true) * 1000);
    }
}
