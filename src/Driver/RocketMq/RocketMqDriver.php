<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RocketMq;

use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\RequestException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Psr\Log\LoggerInterface;

/**
 * Synchronous short-polling pull consumer and producer over the RocketMQ 4.x remoting protocol.
 *
 * Offsets are tracked in memory per driver instance and committed through
 * UPDATE_CONSUMER_OFFSET on ack/reject; QUERY_CONSUMER_OFFSET is not used.
 */
final class RocketMqDriver implements DriverInterface
{
    private const ROUTE_TTL = 30.0;
    private const NEGATIVE_ROUTE_TTL = 0.5;
    private const EMPTY_POLL_SLEEP_US = 20000;
    private const MAX_EMPTY_POLL_SLEEP_US = 50000;

    /** Requeue delay level 1 = 1 second; the broker's smallest retry delay. */
    private const REQUEUE_DELAY_LEVEL = 1;

    private ?RocketMqConnection $nameserver = null;

    /** @var array<string, RocketMqConnection> */
    private array $connections = [];

    /** @var array<string, true> */
    private array $heartbeats = [];

    /**
     * @var array<string, array{
     *     route: null|array{brokers: array<string, string>, queues: list<array{broker: string, readQueueNums: int, writeQueueNums: int, perm: int}>},
     *     fallback: bool,
     *     expires: float
     * }>
     */
    private array $routes = [];

    /** @var array<string, array<string, int>> topic => offset key => next pull offset */
    private array $offsets = [];

    /** @var array<string, array<string, int>> topic => offset key => committed offset */
    private array $committed = [];

    /** @var array<string, list<array<string, mixed>>> destination => decoded messages */
    private array $buffer = [];

    /** @var array<string, int> destination => round-robin start index */
    private array $cursor = [];

    private string $clientId;

    private string $retryTopic;

    private int $publishCounter = 0;

    public function __construct(
        private RocketMqConfig $config,
        private ?LoggerInterface $logger = null,
    ) {
        $this->clientId = sprintf(
            '%s@%d#%s',
            gethostname() ?: 'php',
            (int) getmypid(),
            bin2hex(random_bytes(4)),
        );
        $this->retryTopic = RocketMqProtocol::retryTopic($config->group());
    }

    public function publish(string $destination, Message $message, array $options = []): PublishResult
    {
        $topic = $this->config->destination($destination);
        $route = $this->route($topic, true);
        if ($route === null) {
            throw new RequestException(sprintf('RocketMQ topic [%s] has no route.', $topic));
        }

        $queues = self::writableQueues($route);
        if ($queues === []) {
            throw new RequestException(sprintf('RocketMQ topic [%s] has no writable master queue.', $topic));
        }

        $candidates = [];
        $defaultQueueNums = 0;
        foreach ($queues as $queue) {
            $address = $route['brokers'][$queue['broker']] ?? null;
            if ($address === null) {
                continue;
            }

            $defaultQueueNums = max($defaultQueueNums, $queue['writeQueueNums']);
            for ($queueId = 0; $queueId < $queue['writeQueueNums']; $queueId++) {
                $candidates[] = [$address, $queueId];
            }
        }

        if ($candidates === []) {
            throw new RequestException(sprintf('RocketMQ topic [%s] has no reachable writable queue.', $topic));
        }

        $index = $message->key !== null && $message->key !== ''
            ? crc32($message->key) % count($candidates)
            : $this->publishCounter++ % count($candidates);
        [$address, $queueId] = $candidates[$index];

        $uniqKey = bin2hex(random_bytes(16));
        $extFields = [
            'producerGroup' => $this->config->group(),
            'topic' => $topic,
            'defaultTopic' => RocketMqProtocol::DEFAULT_TOPIC,
            'defaultTopicQueueNums' => (string) max(1, $defaultQueueNums),
            'queueId' => (string) $queueId,
            'sysFlag' => '0',
            'bornTimestamp' => (string) (int) floor(microtime(true) * 1000),
            'flag' => '0',
            'properties' => RocketMqMessageCodec::buildProperties($message, $uniqKey),
            'reconsumeTimes' => '0',
            'unitMode' => 'false',
            'batch' => 'false',
        ];

        $response = $this->brokerRequest($address, RocketMqProtocol::SEND_MESSAGE, $extFields, $message->body);
        RocketMqError::throwFor($response, 'send message');

        if ($route['fallback']) {
            // The broker created the topic while accepting this message; refresh its route next time.
            unset($this->routes[$topic]);
        }

        return new PublishResult($topic, $uniqKey, [
            'msgId' => $response->ext('msgId'),
            'queueId' => $response->extInt('queueId', $queueId),
            'queueOffset' => $response->extInt('queueOffset', -1),
        ]);
    }

    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        throw new UnsupportedFeatureException('RocketMQ batch publishing is not supported by this driver.');
    }

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage
    {
        $topic = $this->config->destination($destination);

        $buffered = $this->takeBuffered($topic);
        if ($buffered !== null) {
            return $this->toReceivedMessage($buffered);
        }

        $deadline = $options->timeoutMs === 0 ? null : microtime(true) + $options->timeoutMs / 1000;

        while (true) {
            $queues = $this->consumableQueues($topic);

            if ($queues !== []) {
                $count = count($queues);
                $start = ($this->cursor[$topic] ?? 0) % $count;

                for ($i = 0; $i < $count; $i++) {
                    $index = ($start + $i) % $count;

                    try {
                        $message = $this->pull($topic, $queues[$index], $deadline);
                    } catch (TimeoutException $exception) {
                        if ($deadline !== null && microtime(true) >= $deadline) {
                            return null;
                        }

                        throw $exception;
                    }

                    if ($message !== null) {
                        $this->cursor[$topic] = ($index + 1) % $count;

                        return $this->toReceivedMessage($message);
                    }
                }

                $this->cursor[$topic] = 0;
            }

            if ($deadline === null) {
                usleep(self::EMPTY_POLL_SLEEP_US);
                continue;
            }

            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                return null;
            }

            usleep((int) min(self::MAX_EMPTY_POLL_SLEEP_US, max(1, $remaining * 1_000_000)));
        }
    }

    public function capabilities(): Capabilities
    {
        return new Capabilities(
            ack: true,
            requeue: true,
            delay: true,
            consumerGroups: true,
            batchPublish: false,
            ordering: true,
            headers: true,
            requiresActiveSubscription: false,
        );
    }

    public function close(): void
    {
        $this->nameserver?->close();
        $this->nameserver = null;

        foreach ($this->connections as $connection) {
            $connection->close();
        }

        $this->connections = [];
        $this->heartbeats = [];
        $this->routes = [];
        $this->offsets = [];
        $this->committed = [];
        $this->buffer = [];
        $this->cursor = [];
    }

    /**
     * @return null|array{brokers: array<string, string>, queues: list<array{broker: string, readQueueNums: int, writeQueueNums: int, perm: int}>, fallback: bool}
     */
    private function route(string $topic, bool $allowDefault): ?array
    {
        $cached = $this->routes[$topic] ?? null;
        if ($cached !== null && $cached['expires'] > microtime(true)) {
            return $cached['route'] === null ? null : $cached['route'] + ['fallback' => $cached['fallback']];
        }

        try {
            $response = $this->nameserver()->request(
                RocketMqProtocol::GET_ROUTEINFO_BY_TOPIC,
                ['topic' => $topic, 'topicSysFlag' => '0'],
            );
        } catch (ConnectionException | ProtocolException $exception) {
            $this->nameserver?->close();
            $this->nameserver = null;

            throw $exception;
        }

        if ($response->code() === RocketMqProtocol::TOPIC_NOT_EXIST) {
            $fallbackRoute = null;
            if ($allowDefault && $topic !== RocketMqProtocol::DEFAULT_TOPIC) {
                $fallbackRoute = $this->route(RocketMqProtocol::DEFAULT_TOPIC, false);
            }

            $this->routes[$topic] = [
                'route' => $fallbackRoute,
                'fallback' => $fallbackRoute !== null,
                'expires' => microtime(true) + ($fallbackRoute === null ? self::NEGATIVE_ROUTE_TTL : self::ROUTE_TTL),
            ];

            return $fallbackRoute === null ? null : $fallbackRoute + ['fallback' => true];
        }

        RocketMqError::throwFor($response, 'route lookup');

        $parsed = $response->body() === '' ? null : RocketMqRoute::parse($response->body());
        if ($parsed === null || $parsed['queues'] === []) {
            $this->routes[$topic] = ['route' => null, 'fallback' => false, 'expires' => microtime(true) + self::NEGATIVE_ROUTE_TTL];

            return null;
        }

        $this->routes[$topic] = ['route' => $parsed, 'fallback' => false, 'expires' => microtime(true) + self::ROUTE_TTL];

        return $parsed + ['fallback' => false];
    }

    /**
     * @param array{brokers: array<string, string>, queues: list<array{broker: string, readQueueNums: int, writeQueueNums: int, perm: int}>, fallback: bool} $route
     * @return list<array{broker: string, readQueueNums: int, writeQueueNums: int, perm: int}>
     */
    private static function writableQueues(array $route): array
    {
        return array_values(array_filter(
            $route['queues'],
            static fn (array $queue): bool => ($queue['perm'] & RocketMqProtocol::PERM_WRITE) !== 0
                && $queue['writeQueueNums'] > 0,
        ));
    }

    /**
     * @return list<array{address: string, topic: string, queueId: int, retry: bool}>
     */
    private function consumableQueues(string $topic): array
    {
        return array_merge(
            $this->readableQueues($topic, false),
            $this->readableQueues($this->retryTopic, true),
        );
    }

    /**
     * @return list<array{address: string, topic: string, queueId: int, retry: bool}>
     */
    private function readableQueues(string $topic, bool $retry): array
    {
        $route = $this->route($topic, false);
        if ($route === null) {
            return [];
        }

        $queues = [];
        foreach ($route['queues'] as $queueData) {
            $address = $route['brokers'][$queueData['broker']] ?? null;
            if ($address === null || ($queueData['perm'] & RocketMqProtocol::PERM_READ) === 0) {
                continue;
            }

            for ($queueId = 0; $queueId < $queueData['readQueueNums']; $queueId++) {
                $queues[] = [
                    'address' => $address,
                    'topic' => $topic,
                    'queueId' => $queueId,
                    'retry' => $retry,
                ];
            }
        }

        return $queues;
    }

    /**
     * @param array{address: string, topic: string, queueId: int, retry: bool} $queue
     * @return null|array<string, mixed>
     */
    private function pull(string $destination, array $queue, ?float $deadline): ?array
    {
        $key = self::offsetKey($queue['address'], $queue['queueId']);
        $offset = $this->offsets[$queue['topic']][$key] ?? $this->initialOffset($queue);
        $requestDeadline = $deadline === null ? null : min($deadline, microtime(true) + $this->config->timeout());

        $response = $this->brokerRequest($queue['address'], RocketMqProtocol::PULL_MESSAGE, [
            'consumerGroup' => $this->config->group(),
            'topic' => $queue['topic'],
            'queueId' => (string) $queue['queueId'],
            'queueOffset' => (string) $offset,
            'maxMsgNums' => (string) $this->config->pullBatchSize(),
            // PullSysFlag: SUBSCRIPTION (4) makes the broker accept the inline subscription below.
            'sysFlag' => '4',
            'commitOffset' => '0',
            'suspendTimeoutMillis' => '0',
            'subscription' => '',
            'subVersion' => '0',
            'expressionType' => 'TAG',
        ], '', $requestDeadline, true);

        $code = $response->code();
        if ($code === RocketMqProtocol::PULL_NOT_FOUND || $code === RocketMqProtocol::PULL_RETRY_IMMEDIATELY) {
            return null;
        }

        if ($code === RocketMqProtocol::PULL_OFFSET_MOVED) {
            $this->offsets[$queue['topic']][$key] = $response->extInt('nextBeginOffset', $offset);

            return null;
        }

        RocketMqError::throwFor($response, 'pull message');

        $messages = RocketMqMessageCodec::decodeMessages($response->body());
        $this->offsets[$queue['topic']][$key] = $response->extInt('nextBeginOffset', $offset + count($messages));

        $candidates = [];
        foreach ($messages as $message) {
            $entry = $message;
            $entry['pullTopic'] = $queue['topic'];
            $entry['address'] = $queue['address'];

            // Retried messages carry their original topic; everything else belongs to the pulled topic.
            $origin = $queue['retry']
                ? ($entry['properties']['RETRY_TOPIC'] ?? $destination)
                : $entry['topic'];

            if ($origin === $destination) {
                $candidates[] = $entry;
            } else {
                $this->buffer[$origin][] = $entry;
            }
        }

        if ($candidates === []) {
            return null;
        }

        $first = array_shift($candidates);
        foreach ($candidates as $candidate) {
            $this->buffer[$destination][] = $candidate;
        }

        return $first;
    }

    /**
     * @param array{address: string, topic: string, queueId: int, retry: bool} $queue
     */
    private function initialOffset(array $queue): int
    {
        // Retry queues always start at the min offset so a message sent back before the
        // retry topic was first seen is not skipped by consume_from=last.
        $code = $queue['retry'] || $this->config->consumeFromFirst()
            ? RocketMqProtocol::GET_MIN_OFFSET
            : RocketMqProtocol::GET_MAX_OFFSET;

        $response = $this->brokerRequest($queue['address'], $code, [
            'topic' => $queue['topic'],
            'queueId' => (string) $queue['queueId'],
        ], '', null, true);
        RocketMqError::throwFor($response, 'query offset');

        $offset = $response->extInt('offset');
        $this->offsets[$queue['topic']][self::offsetKey($queue['address'], $queue['queueId'])] = $offset;

        return $offset;
    }

    /**
     * @param array<string, mixed> $entry
     */
    private function toReceivedMessage(array $entry): ReceivedMessage
    {
        $properties = $entry['properties'];
        $address = $entry['address'];
        $topic = $entry['pullTopic'];
        $queueId = $entry['queueId'];
        $queueOffset = $entry['queueOffset'];
        $settled = false;

        $commitOffset = $queueOffset + 1;

        return new ReceivedMessage(
            body: $entry['body'],
            headers: RocketMqMessageCodec::userProperties($properties),
            id: $properties['UNIQ_KEY'] ?? null,
            key: $properties['KEYS'] ?? null,
            attempts: $entry['reconsumeTimes'] + 1,
            ackCallback: function () use (&$settled, $address, $topic, $queueId, $commitOffset): void {
                if ($settled) {
                    return;
                }

                $settled = true;
                $this->commit($address, $topic, $queueId, $commitOffset);
            },
            rejectCallback: function (bool $requeue) use (
                &$settled,
                $entry,
                $address,
                $topic,
                $queueId,
                $queueOffset,
                $commitOffset,
            ): void {
                if ($settled) {
                    return;
                }

                $settled = true;

                if (!$requeue) {
                    $this->commit($address, $topic, $queueId, $commitOffset);

                    return;
                }

                $originTopic = $entry['properties']['RETRY_TOPIC'] ?? $topic;

                try {
                    $this->sendBack($address, $entry, $originTopic);
                } catch (\Throwable $exception) {
                    // Keep the message visible to this consumer instead of losing it.
                    $rewindKey = self::offsetKey($address, $queueId);
                    $this->offsets[$topic][$rewindKey] = min(
                        $this->offsets[$topic][$rewindKey] ?? $queueOffset,
                        $queueOffset,
                    );

                    throw $exception;
                }

                $this->commit($address, $topic, $queueId, $commitOffset);
                unset($this->routes[$this->retryTopic]);
            },
            raw: $entry,
        );
    }

    /**
     * @param array<string, mixed> $entry
     */
    private function sendBack(string $address, array $entry, string $originTopic): void
    {
        $response = $this->brokerRequest($address, RocketMqProtocol::CONSUMER_SEND_MSG_BACK, [
            'offset' => (string) $entry['physicalOffset'],
            'group' => $this->config->group(),
            'delayLevel' => (string) self::REQUEUE_DELAY_LEVEL,
            'originMsgId' => $entry['properties']['UNIQ_KEY'] ?? '',
            'originTopic' => $originTopic,
            'unitMode' => 'false',
            'maxReconsumeTimes' => '0',
        ], '', null, true);

        RocketMqError::throwFor($response, 'send message back');
    }

    private function commit(string $address, string $topic, int $queueId, int $offset): void
    {
        $key = self::offsetKey($address, $queueId);
        if ($offset <= ($this->committed[$topic][$key] ?? -1)) {
            return;
        }

        $response = $this->brokerRequest($address, RocketMqProtocol::UPDATE_CONSUMER_OFFSET, [
            'consumerGroup' => $this->config->group(),
            'topic' => $topic,
            'queueId' => (string) $queueId,
            'commitOffset' => (string) $offset,
        ], '', null, true);
        RocketMqError::throwFor($response, 'update consumer offset');

        $this->committed[$topic][$key] = $offset;
    }

    /**
     * @param array<string, string> $extFields
     */
    private function brokerRequest(
        string $address,
        int $code,
        array $extFields,
        string $body = '',
        ?float $deadline = null,
        bool $consumer = false,
    ): RocketMqCommand {
        try {
            $connection = $consumer ? $this->consumerConnection($address) : $this->connection($address);

            return $connection->request($code, $extFields, $body, $deadline);
        } catch (ConnectionException | ProtocolException $exception) {
            $this->forgetConnection($address);

            throw $exception;
        }
    }

    private function connection(string $address): RocketMqConnection
    {
        if (isset($this->connections[$address])) {
            return $this->connections[$address];
        }

        [$host, $port] = RocketMqConfig::parseAddress($address, RocketMqConfig::DEFAULT_BROKER_PORT);
        $this->logger?->debug('Connecting to RocketMQ broker.', ['address' => $address]);

        $connection = RocketMqConnection::connect($host, $port, $this->config->timeout());
        $this->connections[$address] = $connection;

        return $connection;
    }

    private function consumerConnection(string $address): RocketMqConnection
    {
        $connection = $this->connection($address);
        if (!isset($this->heartbeats[$address])) {
            try {
                $response = $connection->request(RocketMqProtocol::HEART_BEAT, [], $this->heartbeatBody());
            } catch (ConnectionException | ProtocolException $exception) {
                $this->forgetConnection($address);

                throw $exception;
            }

            RocketMqError::throwFor($response, 'heartbeat');
            $this->heartbeats[$address] = true;
        }

        return $connection;
    }

    private function forgetConnection(string $address): void
    {
        if (!isset($this->connections[$address])) {
            return;
        }

        $this->connections[$address]->close();
        unset($this->connections[$address], $this->heartbeats[$address]);
    }

    private function nameserver(): RocketMqConnection
    {
        if ($this->nameserver === null) {
            $this->logger?->debug('Connecting to RocketMQ nameserver.', [
                'host' => $this->config->nameserverHost(),
                'port' => $this->config->nameserverPort(),
            ]);

            $this->nameserver = RocketMqConnection::connect(
                $this->config->nameserverHost(),
                $this->config->nameserverPort(),
                $this->config->timeout(),
            );
        }

        return $this->nameserver;
    }

    private function heartbeatBody(): string
    {
        $data = [
            'clientID' => $this->clientId,
            'consumerDataSet' => [[
                'groupName' => $this->config->group(),
                'consumeType' => 'CONSUME_ACTIVELY',
                'messageModel' => 'CLUSTERING',
                'consumeFromWhere' => $this->config->consumeFromFirst()
                    ? 'CONSUME_FROM_FIRST_OFFSET'
                    : 'CONSUME_FROM_LAST_OFFSET',
                'subscriptionDataSet' => [],
                'unitMode' => false,
            ]],
        ];

        return (string) json_encode($data, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
    }

    /**
     * @return null|array<string, mixed>
     */
    private function takeBuffered(string $topic): ?array
    {
        if (empty($this->buffer[$topic])) {
            unset($this->buffer[$topic]);

            return null;
        }

        $entry = array_shift($this->buffer[$topic]);
        if (empty($this->buffer[$topic])) {
            unset($this->buffer[$topic]);
        }

        return $entry;
    }

    private static function offsetKey(string $address, int $queueId): string
    {
        return $address . '#' . $queueId;
    }
}
