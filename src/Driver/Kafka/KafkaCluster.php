<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Psr\Log\LoggerInterface;

/**
 * Broker pool plus topic metadata cache.
 */
final class KafkaCluster
{
    /** @var list<array{host: string, port: int}> */
    private array $seeds;

    /** @var array<string, KafkaBroker> */
    private array $connections = [];

    /** @var array<int, array{host: string, port: int}> */
    private array $brokers = [];

    /** @var array<string, array{error: int, partitions: array<int, array{leader: int, error: int}>}> */
    private array $topics = [];

    public function __construct(
        private KafkaConfig $config,
        private ?LoggerInterface $logger = null,
    ) {
        $this->seeds = $config->brokers;
    }

    public function broker(string $host, int $port): KafkaBroker
    {
        $key = $host . ':' . $port;
        $broker = $this->connections[$key] ?? null;

        if ($broker !== null && $broker->isOpen()) {
            return $broker;
        }

        return $this->connections[$key] = new KafkaBroker($host, $port, $this->config, $this->logger);
    }

    public function brokerById(int $id): KafkaBroker
    {
        if (!isset($this->brokers[$id])) {
            $this->refreshMetadata();
        }

        if (!isset($this->brokers[$id])) {
            throw new ProtocolException(sprintf('Kafka broker id [%d] is not listed in cluster metadata.', $id));
        }

        $address = $this->brokers[$id];

        return $this->broker($address['host'], $address['port']);
    }

    public function anyBroker(): KafkaBroker
    {
        foreach ($this->connections as $broker) {
            if ($broker->isOpen()) {
                return $broker;
            }
        }

        $previous = null;
        foreach ($this->seeds as $seed) {
            try {
                return $this->broker($seed['host'], $seed['port']);
            } catch (ConnectionException $exception) {
                $previous = $exception;
                $this->logger?->debug('Kafka seed broker is unavailable.', [
                    'broker' => $seed['host'] . ':' . $seed['port'],
                    'error' => $exception->getMessage(),
                ]);
            }
        }

        throw new ConnectionException(
            'Unable to connect to any configured Kafka broker.',
            0,
            $previous,
        );
    }

    /**
     * @param list<string> $topics
     */
    public function refreshMetadata(array $topics = []): void
    {
        $result = $this->anyBroker()->metadata($topics);

        $this->brokers = $result['brokers'];
        foreach ($result['topics'] as $name => $info) {
            $this->topics[$name] = $info;
        }
    }

    /**
     * @return array{error: int, partitions: array<int, array{leader: int, error: int}>}
     */
    public function topic(string $name): array
    {
        $deadline = microtime(true) + min($this->config->timeout, 5.0);

        while (true) {
            $info = $this->topics[$name] ?? null;
            if ($info === null || $info['error'] !== 0 || !$this->leadersReady($info)) {
                $this->refreshMetadata([$name]);
                $info = $this->topics[$name] ?? null;
            }

            if ($info !== null && $info['error'] === 0 && $info['partitions'] !== [] && $this->leadersReady($info)) {
                return $info;
            }

            if (microtime(true) >= $deadline) {
                $error = $info['error'] ?? 3;
                throw new ProtocolException(sprintf(
                    'Kafka topic [%s] is not available: %s (%d).',
                    $name,
                    KafkaErrors::name($error),
                    $error,
                ));
            }

            usleep(100_000);
        }
    }

    /**
     * @return array<int, int> partition index => leader broker id
     */
    public function partitionsFor(string $topic): array
    {
        $partitions = [];
        foreach ($this->topic($topic)['partitions'] as $index => $info) {
            $partitions[$index] = $info['leader'];
        }

        return $partitions;
    }

    public function leaderId(string $topic, int $partition): int
    {
        $info = $this->topic($topic);
        if (!isset($info['partitions'][$partition])) {
            throw new ProtocolException(sprintf(
                'Kafka topic [%s] has no partition [%d].',
                $topic,
                $partition,
            ));
        }

        return $info['partitions'][$partition]['leader'];
    }

    public function coordinator(string $groupId): KafkaBroker
    {
        $attempts = 0;

        while (true) {
            $result = $this->anyBroker()->findCoordinator($groupId);
            if ($result['error'] === 0) {
                return $this->broker($result['host'], $result['port']);
            }

            if (in_array($result['error'], [14, 15, 16], true) && $attempts < 4) {
                $attempts++;
                usleep(100_000);
                continue;
            }

            throw new ProtocolException(sprintf(
                'Kafka FindCoordinator for group [%s] failed: %s (%d)%s',
                $groupId,
                KafkaErrors::name($result['error']),
                $result['error'],
                $result['message'] !== null ? ' - ' . $result['message'] : '.',
            ));
        }
    }

    public function close(): void
    {
        foreach ($this->connections as $broker) {
            $broker->close();
        }

        $this->connections = [];
        $this->brokers = [];
        $this->topics = [];
    }

    /**
     * @param array{partitions: array<int, array{leader: int, error: int}>} $info
     */
    private function leadersReady(array $info): bool
    {
        foreach ($info['partitions'] as $partition) {
            if ($partition['error'] !== 0 || $partition['leader'] < 0) {
                return false;
            }
        }

        return true;
    }
}
