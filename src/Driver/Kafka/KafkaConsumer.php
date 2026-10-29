<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Psr\Log\LoggerInterface;

/**
 * Classic consumer-group session: join/sync, heartbeat, offset commit and fetch buffers.
 */
final class KafkaConsumer
{
    private string $memberId = '';
    private int $generation = -1;
    private string $protocolName = 'range';
    private bool $joined = false;
    private bool $needsRejoin = true;
    private bool $closed = false;
    private ?KafkaBroker $coordinator = null;
    private float $lastHeartbeat = 0.0;

    /** @var array<string, true> */
    private array $subscription = [];

    /** @var array<string, list<int>> */
    private array $assignment = [];

    /** @var array<string, array<int, int>> topic => partition => next fetch offset */
    private array $positions = [];

    /** @var array<string, array<int, list<array{offset: int, timestamp: int, key: ?string, value: ?string, headers: array<string, ?string>}>>> */
    private array $buffers = [];

    public function __construct(
        private KafkaCluster $cluster,
        private KafkaConfig $config,
        private ?LoggerInterface $logger = null,
    ) {
    }

    public function receive(string $topic, int $timeoutMs): ?ReceivedMessage
    {
        if ($this->closed) {
            throw new ProtocolException('Kafka consumer is closed.');
        }

        if (!isset($this->subscription[$topic])) {
            $this->subscription[$topic] = true;
            $this->needsRejoin = true;
        }

        $deadline = $timeoutMs > 0 ? microtime(true) + $timeoutMs / 1000.0 : null;

        while (true) {
            $this->ensureJoined();
            $this->maybeHeartbeat();

            $message = $this->nextBuffered($topic);
            if ($message !== null) {
                return $message;
            }

            $remainingMs = $deadline === null ? null : (int) floor(($deadline - microtime(true)) * 1000);
            if ($remainingMs !== null && $remainingMs <= 0) {
                return null;
            }

            $this->fetch($topic, $deadline);
        }
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;

        if ($this->joined && $this->memberId !== '' && $this->coordinator !== null) {
            try {
                $this->coordinator->leaveGroup($this->config->groupId, $this->memberId);
            } catch (\Throwable $exception) {
                $this->logger?->debug('Kafka LeaveGroup failed while closing.', [
                    'error' => $exception->getMessage(),
                ]);
            }
        }

        $this->joined = false;
    }

    private function ensureJoined(): void
    {
        if ($this->joined && !$this->needsRejoin) {
            return;
        }

        $topics = array_keys($this->subscription);
        sort($topics);
        if ($topics === []) {
            throw new ProtocolException('Kafka consumer has no subscribed topics.');
        }

        $previousAssignment = $this->assignment;
        $metadata = ConsumerProtocol::encodeSubscription($topics);
        $attempts = 0;

        while (true) {
            $this->coordinator = $this->cluster->coordinator($this->config->groupId);

            $join = $this->coordinator->joinGroup(
                $this->config->groupId,
                $this->memberId,
                $this->protocolName,
                $metadata,
                $this->config->sessionTimeoutMs,
                $this->config->sessionTimeoutMs,
            );

            $error = $join['error'];
            if ($error !== 0) {
                if ($error === 79 && $join['member_id'] !== '') {
                    $this->memberId = $join['member_id'];
                    if ($attempts++ < 4) {
                        continue;
                    }
                }

                if ($error === 25) {
                    $this->memberId = '';
                }

                if ((KafkaErrors::isGroupStateError($error) || in_array($error, [14, 15, 16], true)) && $attempts++ < 4) {
                    usleep(100_000);
                    continue;
                }

                KafkaErrors::assertNone($error, 'JoinGroup');
            }

            $this->memberId = $join['member_id'];
            $this->generation = $join['generation'];
            if ($join['protocol'] !== '') {
                $this->protocolName = $join['protocol'];
            }

            $assignments = [];
            if ($join['leader'] === $this->memberId) {
                $assignments = $this->planAssignments($join['members']);
            }

            $sync = $this->coordinator->syncGroup(
                $this->config->groupId,
                $this->generation,
                $this->memberId,
                $assignments,
            );

            if ($sync['error'] !== 0) {
                $error = $sync['error'];
                if ($error === 25) {
                    $this->memberId = '';
                }

                if ((KafkaErrors::isGroupStateError($error) || in_array($error, [14, 15, 16], true)) && $attempts++ < 4) {
                    usleep(100_000);
                    continue;
                }

                KafkaErrors::assertNone($error, 'SyncGroup');
            }

            $this->assignment = ConsumerProtocol::decodeAssignment($sync['assignment']);
            $this->initializePositions($previousAssignment);
            $this->joined = true;
            $this->needsRejoin = false;
            $this->lastHeartbeat = microtime(true);

            return;
        }
    }

    /**
     * @param array<string, string> $members member id => subscription metadata
     * @return array<string, string> member id => assignment metadata
     */
    private function planAssignments(array $members): array
    {
        $subscriptions = [];
        $allTopics = [];

        foreach ($members as $memberId => $metadata) {
            $topics = ConsumerProtocol::decodeSubscription($metadata);
            $subscriptions[$memberId] = $topics;
            foreach ($topics as $topic) {
                $allTopics[$topic] = true;
            }
        }

        if (!isset($subscriptions[$this->memberId])) {
            $subscriptions[$this->memberId] = array_keys($this->subscription);
            foreach ($subscriptions[$this->memberId] as $topic) {
                $allTopics[$topic] = true;
            }
        }

        $partitionCounts = [];
        foreach (array_keys($allTopics) as $topic) {
            $partitionCounts[$topic] = count($this->cluster->partitionsFor($topic));
        }

        $assignments = [];
        foreach (ConsumerProtocol::rangeAssign($subscriptions, $partitionCounts) as $memberId => $assignment) {
            $assignments[$memberId] = ConsumerProtocol::encodeAssignment($assignment);
        }

        return $assignments;
    }

    /**
     * @param array<string, list<int>> $previousAssignment
     */
    private function initializePositions(array $previousAssignment): void
    {
        $fresh = [];
        foreach ($this->assignment as $topic => $partitions) {
            foreach ($partitions as $partition) {
                $wasAssigned = in_array($partition, $previousAssignment[$topic] ?? [], true);
                if (!$wasAssigned || !isset($this->positions[$topic][$partition])) {
                    $fresh[$topic][] = $partition;
                }
            }
        }

        foreach ($this->positions as $topic => $partitions) {
            foreach (array_keys($partitions) as $partition) {
                if (!in_array($partition, $this->assignment[$topic] ?? [], true)) {
                    unset($this->positions[$topic][$partition]);
                }
            }

            if (($this->positions[$topic] ?? []) === []) {
                unset($this->positions[$topic]);
            }
        }

        foreach ($this->buffers as $topic => $partitions) {
            foreach (array_keys($partitions) as $partition) {
                if (!in_array($partition, $this->assignment[$topic] ?? [], true)) {
                    unset($this->buffers[$topic][$partition]);
                }
            }

            if (($this->buffers[$topic] ?? []) === []) {
                unset($this->buffers[$topic]);
            }
        }

        if ($fresh === []) {
            return;
        }

        $committed = $this->coordinatorOrFail()->offsetFetch($this->config->groupId, $fresh);
        KafkaErrors::assertNone($committed['error'], 'OffsetFetch');

        foreach ($fresh as $topic => $partitions) {
            foreach ($partitions as $partition) {
                $offset = $committed['topics'][$topic][$partition]['offset'] ?? -1;
                if ($offset >= 0) {
                    $this->positions[$topic][$partition] = $offset;
                }
            }
        }

        foreach ($fresh as $topic => $partitions) {
            foreach ($partitions as $partition) {
                if (!isset($this->positions[$topic][$partition])) {
                    $this->positions[$topic][$partition] = $this->resetPosition($topic, $partition);
                }
            }
        }
    }

    private function maybeHeartbeat(): void
    {
        if (!$this->joined || $this->coordinator === null) {
            return;
        }

        $interval = $this->config->sessionTimeoutMs / 3000.0;
        if (microtime(true) - $this->lastHeartbeat < $interval) {
            return;
        }

        $this->lastHeartbeat = microtime(true);
        $error = $this->coordinator->heartbeat(
            $this->config->groupId,
            $this->generation,
            $this->memberId,
        );

        if ($error === 0) {
            return;
        }

        if ($error === 25) {
            $this->memberId = '';
        }

        if (KafkaErrors::isGroupStateError($error) || in_array($error, [14, 15, 16], true)) {
            $this->needsRejoin = true;
            if (in_array($error, [14, 15, 16], true)) {
                $this->coordinator = null;
            }

            return;
        }

        KafkaErrors::assertNone($error, 'Heartbeat');
    }

    private function fetch(string $topic, ?float $deadline): void
    {
        $partitions = $this->assignment[$topic] ?? [];
        if ($partitions === []) {
            $this->idle($deadline);

            return;
        }

        $byLeader = [];
        foreach ($partitions as $partition) {
            if (($this->buffers[$topic][$partition] ?? []) !== []) {
                continue;
            }

            $leaderId = $this->cluster->leaderId($topic, $partition);
            $byLeader[$leaderId][$partition] = $this->positions[$topic][$partition] ?? 0;
        }

        if ($byLeader === []) {
            return;
        }

        $remainingLeaders = count($byLeader);
        $sessionSafeWaitMs = max(1, intdiv($this->config->sessionTimeoutMs, 3));

        foreach ($byLeader as $leaderId => $offsets) {
            $waitMs = max(1, min($this->config->fetchMaxWaitMs, $sessionSafeWaitMs));

            if ($deadline !== null) {
                $remainingMs = (int) floor(($deadline - microtime(true)) * 1000);
                if ($remainingMs <= 0) {
                    return;
                }

                $waitMs = min($waitMs, max(1, intdiv($remainingMs, $remainingLeaders)));
            }

            $waitMs = max(1, $waitMs);
            $remainingLeaders--;

            $response = $this->cluster->brokerById($leaderId)->fetch([$topic => $offsets], $waitMs);
            $this->handleFetchResponse($response);
        }
    }

    private function idle(?float $deadline): void
    {
        $sleepMs = $this->config->fetchMaxWaitMs > 0 ? $this->config->fetchMaxWaitMs : 100;

        if ($deadline !== null) {
            $remainingMs = (int) floor(($deadline - microtime(true)) * 1000);
            $sleepMs = min($sleepMs, max(0, $remainingMs));
        }

        if ($sleepMs > 0) {
            usleep($sleepMs * 1000);
        }
    }

    /**
     * @param array<string, array<int, array{error: int, high_watermark: int, records: ?string}>> $response
     */
    private function handleFetchResponse(array $response): void
    {
        foreach ($response as $topic => $partitions) {
            foreach ($partitions as $partition => $result) {
                $error = $result['error'];

                if ($error === 0) {
                    $raw = $result['records'];
                    if ($raw === null || $raw === '') {
                        continue;
                    }

                    $records = RecordBatch::decode(
                        $raw,
                        $this->positions[$topic][$partition] ?? 0,
                        $this->config->maxPollRecords,
                    );

                    if ($records === []) {
                        continue;
                    }

                    $this->buffers[$topic][$partition] = array_merge(
                        $this->buffers[$topic][$partition] ?? [],
                        $records,
                    );

                    $last = $records[count($records) - 1];
                    $this->positions[$topic][$partition] = $last['offset'] + 1;

                    continue;
                }

                if ($error === 1) { // OFFSET_OUT_OF_RANGE
                    $this->positions[$topic][$partition] = $this->resetPosition($topic, $partition);

                    continue;
                }

                if (KafkaErrors::isRetriable($error)) {
                    try {
                        $this->cluster->refreshMetadata([$topic]);
                    } catch (ProtocolException $exception) {
                        $this->logger?->debug('Kafka metadata refresh failed during fetch.', [
                            'topic' => $topic,
                            'error' => $exception->getMessage(),
                        ]);
                    }

                    continue;
                }

                KafkaErrors::assertNone($error, sprintf('Fetch for [%s-%d]', $topic, $partition));
            }
        }
    }

    private function resetPosition(string $topic, int $partition): int
    {
        $timestamp = $this->config->autoOffsetReset === 'earliest' ? -2 : -1;
        $leaderId = $this->cluster->leaderId($topic, $partition);
        $result = $this->cluster->brokerById($leaderId)->listOffsets($topic, [$partition], $timestamp);

        $data = $result[$topic][$partition] ?? null;
        if ($data === null) {
            throw new ProtocolException(sprintf(
                'Kafka ListOffsets returned no offset for [%s-%d].',
                $topic,
                $partition,
            ));
        }

        KafkaErrors::assertNone($data['error'], sprintf('ListOffsets for [%s-%d]', $topic, $partition));

        return $data['offset'];
    }

    private function nextBuffered(string $topic): ?ReceivedMessage
    {
        foreach ($this->assignment[$topic] ?? [] as $partition) {
            $buffer = $this->buffers[$topic][$partition] ?? [];
            if ($buffer === []) {
                continue;
            }

            $record = array_shift($this->buffers[$topic][$partition]);

            return $this->wrap($topic, $partition, $record);
        }

        return null;
    }

    /**
     * @param array{offset: int, timestamp: int, key: ?string, value: ?string, headers: array<string, ?string>} $record
     */
    private function wrap(string $topic, int $partition, array $record): ReceivedMessage
    {
        $settled = false;
        $offset = $record['offset'];

        return new ReceivedMessage(
            body: $record['value'] ?? '',
            headers: $record['headers'],
            id: sprintf('%s-%d-%d', $topic, $partition, $offset),
            key: $record['key'],
            attempts: 1,
            ackCallback: function () use ($topic, $partition, $offset, &$settled): void {
                if ($settled) {
                    return;
                }

                $settled = true;
                $this->commit($topic, $partition, $offset + 1);
            },
            rejectCallback: function (bool $requeue) use ($topic, $partition, $offset, &$settled): void {
                if ($settled) {
                    return;
                }

                $settled = true;

                if ($requeue) {
                    $this->seek($topic, $partition, $offset);

                    return;
                }

                $this->commit($topic, $partition, $offset + 1);
            },
            raw: [
                'topic' => $topic,
                'partition' => $partition,
                'offset' => $offset,
                'timestamp' => $record['timestamp'],
            ],
        );
    }

    private function commit(string $topic, int $partition, int $offset): void
    {
        if (!$this->joined || $this->coordinator === null) {
            throw new ProtocolException('Kafka consumer has no active group membership to commit offsets.');
        }

        $result = $this->coordinator->offsetCommit(
            $this->config->groupId,
            $this->generation,
            $this->memberId,
            [$topic => [$partition => $offset]],
        );

        $error = $result[$topic][$partition] ?? 0;
        if ($error === 0) {
            return;
        }

        if ($error === 25) {
            $this->memberId = '';
        }

        if (KafkaErrors::isGroupStateError($error) || in_array($error, [14, 15, 16], true)) {
            $this->needsRejoin = true;
        }

        KafkaErrors::assertNone($error, sprintf('OffsetCommit for [%s-%d]', $topic, $partition));
    }

    private function seek(string $topic, int $partition, int $offset): void
    {
        $this->buffers[$topic][$partition] = [];
        $this->positions[$topic][$partition] = $offset;
    }

    private function coordinatorOrFail(): KafkaBroker
    {
        if ($this->coordinator === null) {
            $this->coordinator = $this->cluster->coordinator($this->config->groupId);
        }

        return $this->coordinator;
    }
}
