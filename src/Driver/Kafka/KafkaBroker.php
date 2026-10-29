<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Psr\Log\LoggerInterface;

/**
 * One broker endpoint with negotiated API versions.
 */
final class KafkaBroker
{
    private KafkaConnection $connection;

    /** @var array<int, array{0: int, 1: int}> */
    private array $versions = [];

    public function __construct(
        private string $host,
        private int $port,
        private KafkaConfig $config,
        private ?LoggerInterface $logger = null,
    ) {
        $this->connection = $this->open();
        $this->negotiateVersions();
    }

    /**
     * @return array{brokers: array<int, array{host: string, port: int}>, topics: array<string, array{error: int, partitions: array<int, array{leader: int, error: int}>}>}
     */
    public function metadata(array $topics): array
    {
        $this->requireVersion(KafkaProtocol::API_METADATA, KafkaProtocol::VERSION_METADATA);

        return KafkaProtocol::parseMetadataResponse($this->connection->request(
            KafkaProtocol::API_METADATA,
            KafkaProtocol::VERSION_METADATA,
            KafkaProtocol::metadataRequest($topics),
            1,
            0,
            $this->config->timeout,
        ));
    }

    /**
     * @param array<int, string> $partitionBatches
     * @return array<string, array<int, array{error: int, base_offset: int}>>
     */
    public function produce(string $topic, array $partitionBatches, int $acks, int $timeoutMs): array
    {
        $this->requireVersion(KafkaProtocol::API_PRODUCE, KafkaProtocol::VERSION_PRODUCE);

        return KafkaProtocol::parseProduceResponse($this->connection->request(
            KafkaProtocol::API_PRODUCE,
            KafkaProtocol::VERSION_PRODUCE,
            KafkaProtocol::produceRequest($topic, $partitionBatches, $acks, $timeoutMs),
            1,
            0,
            $this->config->timeout,
        ));
    }

    /**
     * @param array<string, array<int, int>> $offsets
     * @return array<string, array<int, array{error: int, high_watermark: int, records: ?string}>>
     */
    public function fetch(array $offsets, int $maxWaitMs, int $maxBytes = 52_428_800, int $partitionMaxBytes = 1_048_576): array
    {
        $this->requireVersion(KafkaProtocol::API_FETCH, KafkaProtocol::VERSION_FETCH);

        return KafkaProtocol::parseFetchResponse($this->connection->request(
            KafkaProtocol::API_FETCH,
            KafkaProtocol::VERSION_FETCH,
            KafkaProtocol::fetchRequest($offsets, $maxWaitMs, $maxBytes, $partitionMaxBytes),
            1,
            0,
            $maxWaitMs / 1000.0 + 1.0,
        ));
    }

    /**
     * @param list<int> $partitions
     * @return array<string, array<int, array{error: int, offset: int}>>
     */
    public function listOffsets(string $topic, array $partitions, int $timestamp): array
    {
        $this->requireVersion(KafkaProtocol::API_LIST_OFFSETS, KafkaProtocol::VERSION_LIST_OFFSETS);

        return KafkaProtocol::parseListOffsetsResponse($this->connection->request(
            KafkaProtocol::API_LIST_OFFSETS,
            KafkaProtocol::VERSION_LIST_OFFSETS,
            KafkaProtocol::listOffsetsRequest($topic, $partitions, $timestamp),
            1,
            0,
            $this->config->timeout,
        ));
    }

    /**
     * @return array{error: int, message: ?string, node_id: int, host: string, port: int}
     */
    public function findCoordinator(string $groupId): array
    {
        $this->requireVersion(KafkaProtocol::API_FIND_COORDINATOR, KafkaProtocol::VERSION_FIND_COORDINATOR);

        return KafkaProtocol::parseFindCoordinatorResponse($this->connection->request(
            KafkaProtocol::API_FIND_COORDINATOR,
            KafkaProtocol::VERSION_FIND_COORDINATOR,
            KafkaProtocol::findCoordinatorRequest($groupId),
            1,
            0,
            $this->config->timeout,
        ));
    }

    /**
     * @return array{error: int, generation: int, protocol: string, leader: string, member_id: string, members: array<string, string>}
     */
    public function joinGroup(
        string $groupId,
        string $memberId,
        string $protocolName,
        string $metadata,
        int $sessionTimeoutMs,
        int $rebalanceTimeoutMs,
    ): array {
        $this->requireVersion(KafkaProtocol::API_JOIN_GROUP, KafkaProtocol::VERSION_JOIN_GROUP);

        return KafkaProtocol::parseJoinGroupResponse($this->connection->request(
            KafkaProtocol::API_JOIN_GROUP,
            KafkaProtocol::VERSION_JOIN_GROUP,
            KafkaProtocol::joinGroupRequest(
                $groupId,
                $memberId,
                $protocolName,
                $metadata,
                $sessionTimeoutMs,
                $rebalanceTimeoutMs,
            ),
            1,
            0,
            $this->config->timeout,
        ));
    }

    /**
     * @param array<string, string> $assignments
     * @return array{error: int, assignment: string}
     */
    public function syncGroup(string $groupId, int $generationId, string $memberId, array $assignments): array
    {
        $this->requireVersion(KafkaProtocol::API_SYNC_GROUP, KafkaProtocol::VERSION_SYNC_GROUP);

        return KafkaProtocol::parseSyncGroupResponse($this->connection->request(
            KafkaProtocol::API_SYNC_GROUP,
            KafkaProtocol::VERSION_SYNC_GROUP,
            KafkaProtocol::syncGroupRequest($groupId, $generationId, $memberId, $assignments),
            1,
            0,
            $this->config->timeout,
        ));
    }

    public function heartbeat(string $groupId, int $generationId, string $memberId): int
    {
        $this->requireVersion(KafkaProtocol::API_HEARTBEAT, KafkaProtocol::VERSION_HEARTBEAT);

        return KafkaProtocol::parseHeartbeatResponse($this->connection->request(
            KafkaProtocol::API_HEARTBEAT,
            KafkaProtocol::VERSION_HEARTBEAT,
            KafkaProtocol::heartbeatRequest($groupId, $generationId, $memberId),
            1,
            0,
            $this->config->timeout,
        ));
    }

    public function leaveGroup(string $groupId, string $memberId): int
    {
        $this->requireVersion(KafkaProtocol::API_LEAVE_GROUP, KafkaProtocol::VERSION_LEAVE_GROUP);

        return KafkaProtocol::parseLeaveGroupResponse($this->connection->request(
            KafkaProtocol::API_LEAVE_GROUP,
            KafkaProtocol::VERSION_LEAVE_GROUP,
            KafkaProtocol::leaveGroupRequest($groupId, $memberId),
            1,
            0,
            $this->config->timeout,
        ));
    }

    /**
     * @param array<string, array<int, int>> $offsets
     * @return array<string, array<int, int>>
     */
    public function offsetCommit(string $groupId, int $generationId, string $memberId, array $offsets): array
    {
        $this->requireVersion(KafkaProtocol::API_OFFSET_COMMIT, KafkaProtocol::VERSION_OFFSET_COMMIT);

        return KafkaProtocol::parseOffsetCommitResponse($this->connection->request(
            KafkaProtocol::API_OFFSET_COMMIT,
            KafkaProtocol::VERSION_OFFSET_COMMIT,
            KafkaProtocol::offsetCommitRequest($groupId, $generationId, $memberId, $offsets),
            1,
            0,
            $this->config->timeout,
        ));
    }

    /**
     * @param array<string, list<int>> $topics
     * @return array{error: int, topics: array<string, array<int, array{offset: int, error: int}>>}
     */
    public function offsetFetch(string $groupId, array $topics): array
    {
        $this->requireVersion(KafkaProtocol::API_OFFSET_FETCH, KafkaProtocol::VERSION_OFFSET_FETCH);

        return KafkaProtocol::parseOffsetFetchResponse($this->connection->request(
            KafkaProtocol::API_OFFSET_FETCH,
            KafkaProtocol::VERSION_OFFSET_FETCH,
            KafkaProtocol::offsetFetchRequest($groupId, $topics),
            1,
            0,
            $this->config->timeout,
        ));
    }

    public function host(): string
    {
        return $this->host;
    }

    public function port(): int
    {
        return $this->port;
    }

    /**
     * @return array{0: int, 1: int}|null
     */
    public function versionRange(int $apiKey): ?array
    {
        return $this->versions[$apiKey] ?? null;
    }

    public function isOpen(): bool
    {
        return $this->connection->isOpen();
    }

    public function close(): void
    {
        $this->connection->close();
    }

    private function negotiateVersions(): void
    {
        try {
            $result = $this->negotiate(0);
        } catch (ConnectionException) {
            // Very old brokers cannot parse an ApiVersions v0 frame only if they closed us; retry v3.
            $this->reconnect();
            $result = ['error' => 35, 'versions' => []];
        }

        if ($result['error'] === 35) {
            if (!$this->connection->isOpen()) {
                $this->reconnect();
            }

            $result = $this->negotiate(3);
        }

        if ($result['error'] !== 0) {
            throw new UnsupportedFeatureException(sprintf(
                'Kafka broker %s:%d rejected ApiVersions negotiation: %s (%d).',
                $this->host,
                $this->port,
                KafkaErrors::name($result['error']),
                $result['error'],
            ));
        }

        if ($result['versions'] === []) {
            throw new UnsupportedFeatureException(sprintf(
                'Kafka broker %s:%d returned an empty ApiVersions response.',
                $this->host,
                $this->port,
            ));
        }

        $this->versions = $result['versions'];
        $this->logger?->debug('Kafka broker negotiated API versions.', [
            'broker' => $this->host . ':' . $this->port,
            'api_keys' => count($this->versions),
        ]);
    }

    /**
     * @return array{error: int, versions: array<int, array{0: int, 1: int}>}
     */
    private function negotiate(int $version): array
    {
        $body = $version >= 3 ? KafkaProtocol::apiVersionsRequest() : '';
        $decoder = $this->connection->request(
            KafkaProtocol::API_API_VERSIONS,
            $version,
            $body,
            $version >= 3 ? 2 : 1,
            0,
            min($this->config->timeout, 5.0),
        );

        try {
            return KafkaProtocol::parseApiVersionsResponse($decoder, $version);
        } catch (ProtocolException) {
            return ['error' => 35, 'versions' => []];
        }
    }

    private function reconnect(): void
    {
        $this->connection->close();
        $this->connection = $this->open();
    }

    private function open(): KafkaConnection
    {
        return KafkaConnection::connect(
            $this->host,
            $this->port,
            $this->config->clientId,
            $this->config->timeout,
            $this->config->tls,
            $this->config->tlsContext,
        );
    }

    private function requireVersion(int $apiKey, int $version): void
    {
        $range = $this->versions[$apiKey] ?? null;
        if ($range === null) {
            throw new UnsupportedFeatureException(sprintf(
                'Kafka broker %s:%d does not advertise API key %d; this driver requires version %d.',
                $this->host,
                $this->port,
                $apiKey,
                $version,
            ));
        }

        [$min, $max] = $range;
        if ($version < $min || $version > $max) {
            throw new UnsupportedFeatureException(sprintf(
                'Kafka broker %s:%d supports API %d versions %d-%d but this driver requires version %d.',
                $this->host,
                $this->port,
                $apiKey,
                $min,
                $max,
                $version,
            ));
        }
    }
}
