<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RabbitMq;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\Connection;
use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\ContentHeader;
use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\Frame;
use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\Protocol;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * RabbitMQ driver speaking AMQP 0-9-1 directly. The destination is the queue
 * name; messages are published to the default exchange and consumed with
 * basic.consume, buffered until receive() returns them.
 */
final class RabbitMqDriver implements DriverInterface
{
    private const QUEUE_NAME_MAX = 255;

    private string $host;
    private int $port;
    private string $username;
    private string $password;
    private string $vhost;
    private int $heartbeat;
    private float $timeout;
    private bool $tls;
    /** @var array<string, mixed> */
    private array $tlsContext;
    private bool $durable;
    private int $prefetch;
    private LoggerInterface $logger;

    private ?Connection $connection = null;

    /** @var array<string, true> */
    private array $declaredQueues = [];

    /** @var array<string, string> destination => consumer tag */
    private array $consumerTags = [];

    /** @var array<string, string> consumer tag => destination */
    private array $tagDestinations = [];

    /** @var array<string, list<ReceivedMessage>> */
    private array $buffers = [];

    public function __construct(ConnectionConfig $config, ?LoggerInterface $logger = null)
    {
        $this->logger = $logger ?? new NullLogger();
        $this->host = $config->requireString('host');
        $this->tls = self::boolOption($config, 'tls', false);
        $this->port = self::portOption($config, $this->tls);
        $this->username = self::stringOption($config, 'username', 'guest');
        $this->password = self::stringOption($config, 'password', 'guest', true);
        $this->vhost = self::stringOption($config, 'vhost', '/');
        $this->heartbeat = self::intOption($config, 'heartbeat', 60, 0, 65535);
        $this->timeout = self::floatOption($config, 'timeout', 5.0);
        $this->durable = self::boolOption($config, 'durable', true);
        $this->prefetch = self::intOption($config, 'prefetch', 1, 0, 65535);
        $this->tlsContext = self::arrayOption($config, 'tls_context', []);
    }

    /**
     * Opens the connection, negotiates the handshake and applies prefetch.
     * Called by the factory so connection and authentication errors surface at
     * Manager::connection() time.
     */
    public function connect(): void
    {
        $this->connection();
    }

    /**
     * @param array<string, mixed> $options
     */
    public function publish(string $destination, Message $message, array $options = []): PublishResult
    {
        if ($message->delayMs !== null) {
            throw new UnsupportedFeatureException(
                'RabbitMQ delayed delivery requires the rabbitmq_delayed_message_exchange plugin; this driver does not implement it.',
            );
        }

        self::assertDestination($destination);
        $connection = $this->connection();
        $this->ensureQueue($destination);

        $id = bin2hex(random_bytes(16));
        $header = ContentHeader::encode(
            strlen($message->body),
            $id,
            $message->key,
            $message->headers,
            $this->durable ? 2 : 1,
        );

        $connection->writeMethod(
            Protocol::CLASS_BASIC,
            Protocol::BASIC_PUBLISH,
            Protocol::basicPublish('', $destination),
            Connection::CHANNEL,
        );
        $connection->writeContent($header, $message->body);

        return new PublishResult($destination, $id, ['message_id' => $id]);
    }

    /**
     * @param list<Message> $messages
     * @param array<string, mixed> $options
     * @return list<PublishResult>
     */
    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        throw new UnsupportedFeatureException('RabbitMQ batch publishing is not implemented; publish messages individually.');
    }

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage
    {
        self::assertDestination($destination);
        $connection = $this->connection();
        $this->ensureQueue($destination);
        $this->ensureConsumer($destination);

        $deadline = $options->timeoutMs > 0 ? microtime(true) + $options->timeoutMs / 1000 : null;

        while (true) {
            if (($this->buffers[$destination] ?? []) !== []) {
                $message = array_shift($this->buffers[$destination]);
                if (($this->buffers[$destination] ?? []) === []) {
                    unset($this->buffers[$destination]);
                }

                return $message;
            }

            $remaining = null;
            if ($deadline !== null) {
                $remaining = $deadline - microtime(true);
                if ($remaining <= 0) {
                    return null;
                }
            }

            $frame = $connection->readFrame($remaining);
            if ($frame === null) {
                return null;
            }

            $method = $this->handleIncomingFrame($frame);
            if ($method !== null) {
                throw new ProtocolException(sprintf(
                    'Unexpected AMQP method %d.%d while waiting for a message.',
                    $method['class_id'],
                    $method['method_id'],
                ));
            }
        }
    }

    public function capabilities(): Capabilities
    {
        return new Capabilities(
            ack: true,
            requeue: true,
            delay: false,
            consumerGroups: false,
            batchPublish: false,
            ordering: false,
            headers: true,
        );
    }

    public function close(): void
    {
        $this->connection?->close();
        $this->connection = null;
        $this->declaredQueues = [];
        $this->consumerTags = [];
        $this->tagDestinations = [];
        $this->buffers = [];
    }

    private function connection(): Connection
    {
        if ($this->connection !== null) {
            if (!$this->connection->isOpen()) {
                throw new ConnectionException('The RabbitMQ connection is closed.');
            }

            return $this->connection;
        }

        $connection = new Connection(
            $this->host,
            $this->port,
            $this->username,
            $this->password,
            $this->vhost,
            $this->heartbeat,
            $this->timeout,
            $this->tls,
            $this->tlsContext,
            $this->logger,
        );

        $this->connection = $connection;
        try {
            $connection->connect();
            $connection->writeMethod(
                Protocol::CLASS_BASIC,
                Protocol::BASIC_QOS,
                Protocol::basicQos($this->prefetch),
                Connection::CHANNEL,
            );
            $this->awaitMethod(Protocol::CLASS_BASIC, Protocol::BASIC_QOS_OK);
        } catch (\Throwable $exception) {
            $connection->close();
            $this->connection = null;
            throw $exception;
        }

        return $connection;
    }

    private function ensureQueue(string $destination): void
    {
        if (isset($this->declaredQueues[$destination])) {
            return;
        }

        $this->connection()->writeMethod(
            Protocol::CLASS_QUEUE,
            Protocol::QUEUE_DECLARE,
            Protocol::queueDeclare($destination, $this->durable),
            Connection::CHANNEL,
        );
        $this->awaitMethod(Protocol::CLASS_QUEUE, Protocol::QUEUE_DECLARE_OK);
        $this->declaredQueues[$destination] = true;
    }

    private function ensureConsumer(string $destination): void
    {
        if (isset($this->consumerTags[$destination])) {
            return;
        }

        $tag = 'messaging-' . bin2hex(random_bytes(8));
        $this->connection()->writeMethod(
            Protocol::CLASS_BASIC,
            Protocol::BASIC_CONSUME,
            Protocol::basicConsume($destination, $tag),
            Connection::CHANNEL,
        );
        $this->awaitMethod(Protocol::CLASS_BASIC, Protocol::BASIC_CONSUME_OK);
        $this->consumerTags[$destination] = $tag;
        $this->tagDestinations[$tag] = $destination;
    }

    /**
     * Waits for a method reply, processing deliveries and cancellations that
     * arrive for other consumers in the meantime.
     *
     * @return array<string, mixed>
     */
    private function awaitMethod(int $classId, int $methodId): array
    {
        $deadline = microtime(true) + $this->timeout;

        while (true) {
            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                throw new TimeoutException(sprintf('Timed out waiting for AMQP method %d.%d.', $classId, $methodId));
            }

            $frame = $this->connection()->readFrame($remaining);
            if ($frame === null) {
                throw new TimeoutException(sprintf('Timed out waiting for AMQP method %d.%d.', $classId, $methodId));
            }

            $method = $this->handleIncomingFrame($frame);
            if ($method === null) {
                continue;
            }

            if ($method['class_id'] !== $classId || $method['method_id'] !== $methodId) {
                throw new ProtocolException(sprintf(
                    'Expected AMQP method %d.%d, received %d.%d.',
                    $classId,
                    $methodId,
                    $method['class_id'],
                    $method['method_id'],
                ));
            }

            return $method['args'];
        }
    }

    /**
     * @param array{type: int, channel: int, payload: string} $frame
     * @return array{class_id: int, method_id: int, args: array<string, mixed>}|null
     *         null when the frame was fully handled (delivery, cancel, heartbeat)
     */
    private function handleIncomingFrame(array $frame): ?array
    {
        if ($frame['type'] !== Frame::TYPE_METHOD) {
            throw new ProtocolException(sprintf(
                'Unexpected AMQP frame type [%d] outside a message.',
                $frame['type'],
            ));
        }

        $method = Protocol::decodeMethod($frame['payload']);
        $this->throwOnCloseMethod($method);

        if ($frame['channel'] === 0) {
            if ($method['class_id'] === Protocol::CLASS_CONNECTION) {
                switch ($method['method_id']) {
                    case Protocol::CONNECTION_CLOSE_OK:
                        throw new ConnectionException('RabbitMQ closed the connection.');
                    case Protocol::CONNECTION_BLOCKED:
                        $this->logger->warning('RabbitMQ blocked the connection.', [
                            'reason' => $method['args']['reason'] ?? '',
                        ]);

                        return null;
                    case Protocol::CONNECTION_UNBLOCKED:
                        return null;
                }
            }

            throw new ProtocolException(sprintf(
                'Unexpected AMQP method %d.%d on channel 0.',
                $method['class_id'],
                $method['method_id'],
            ));
        }

        if ($frame['channel'] !== Connection::CHANNEL) {
            throw new ProtocolException(sprintf('Unexpected AMQP channel [%d].', $frame['channel']));
        }

        if ($method['class_id'] === Protocol::CLASS_CHANNEL && $method['method_id'] === Protocol::CHANNEL_CLOSE_OK) {
            return null;
        }

        if ($method['class_id'] === Protocol::CLASS_BASIC) {
            switch ($method['method_id']) {
                case Protocol::BASIC_DELIVER:
                    $this->readDelivery($method['args']);

                    return null;
                case Protocol::BASIC_CANCEL:
                    $this->forgetConsumer((string) ($method['args']['consumer_tag'] ?? ''));

                    return null;
                case Protocol::BASIC_ACK:
                case Protocol::BASIC_NACK:
                    // Publisher confirms are not enabled, but tolerate them.
                    return null;
            }
        }

        return $method;
    }

    /**
     * Reads the content header and body frames following basic.deliver and
     * buffers the assembled message for receive().
     *
     * @param array<string, mixed> $args
     */
    private function readDelivery(array $args): void
    {
        $headerFrame = $this->readContentFrame();
        if ($headerFrame['type'] !== Frame::TYPE_HEADER) {
            throw new ProtocolException('Expected a content header frame after basic.deliver.');
        }

        $header = ContentHeader::decode($headerFrame['payload']);
        $bodySize = (int) $header['body_size'];
        if ($bodySize < 0) {
            throw new ProtocolException('The message body size must be zero or positive.');
        }

        $body = '';
        while (strlen($body) < $bodySize) {
            $frame = $this->readContentFrame();
            if ($frame['type'] !== Frame::TYPE_BODY) {
                throw new ProtocolException('Expected a content body frame.');
            }

            $body .= $frame['payload'];
        }

        if (strlen($body) !== $bodySize) {
            throw new ProtocolException(sprintf(
                'The message body is %d bytes but the content header declared %d.',
                strlen($body),
                $bodySize,
            ));
        }

        $consumerTag = (string) ($args['consumer_tag'] ?? '');
        $destination = $this->tagDestinations[$consumerTag] ?? null;
        if ($destination === null) {
            throw new ProtocolException(sprintf('Received a delivery for unknown consumer [%s].', $consumerTag));
        }

        $properties = $header['properties'];
        $headers = $properties['headers'] ?? [];
        if (!is_array($headers)) {
            $headers = [];
        }

        $id = isset($properties['message_id']) ? (string) $properties['message_id'] : null;
        $key = isset($properties['correlation_id'])
            ? (string) $properties['correlation_id']
            : (string) ($args['routing_key'] ?? '');
        if ($key === '') {
            $key = null;
        }

        $deliveryTag = (int) ($args['delivery_tag'] ?? 0);

        $this->buffers[$destination][] = new ReceivedMessage(
            $body,
            $headers,
            $id,
            $key,
            1,
            function () use ($deliveryTag): void {
                $this->connection()->writeMethod(
                    Protocol::CLASS_BASIC,
                    Protocol::BASIC_ACK,
                    Protocol::basicAck($deliveryTag),
                    Connection::CHANNEL,
                );
            },
            function (bool $requeue) use ($deliveryTag): void {
                $this->connection()->writeMethod(
                    Protocol::CLASS_BASIC,
                    Protocol::BASIC_NACK,
                    Protocol::basicNack($deliveryTag, $requeue),
                    Connection::CHANNEL,
                );
            },
            [
                'consumer_tag' => $consumerTag,
                'delivery_tag' => $deliveryTag,
                'redelivered' => (bool) ($args['redelivered'] ?? false),
                'exchange' => (string) ($args['exchange'] ?? ''),
                'routing_key' => (string) ($args['routing_key'] ?? ''),
                'properties' => $properties,
            ],
        );
    }

    /**
     * @return array{type: int, channel: int, payload: string}
     */
    private function readContentFrame(): array
    {
        $frame = $this->connection()->readFrame($this->timeout);
        if ($frame === null) {
            throw new TimeoutException('Timed out while reading the message content.');
        }

        if ($frame['channel'] !== Connection::CHANNEL) {
            throw new ProtocolException(sprintf('Unexpected AMQP channel [%d] in message content.', $frame['channel']));
        }

        if ($frame['type'] === Frame::TYPE_METHOD) {
            $method = Protocol::decodeMethod($frame['payload']);
            $this->throwOnCloseMethod($method);

            throw new ProtocolException('Unexpected AMQP method between content frames.');
        }

        return $frame;
    }

    /**
     * @param array{class_id: int, method_id: int, args: array<string, mixed>} $method
     */
    private function throwOnCloseMethod(array $method): void
    {
        if ($method['class_id'] === Protocol::CLASS_CONNECTION && $method['method_id'] === Protocol::CONNECTION_CLOSE) {
            try {
                $this->connection?->writeMethod(Protocol::CLASS_CONNECTION, Protocol::CONNECTION_CLOSE_OK);
            } catch (\Throwable) {
                // The peer may already be gone.
            }

            throw Protocol::connectionCloseException($method['args']);
        }

        if ($method['class_id'] === Protocol::CLASS_CHANNEL && $method['method_id'] === Protocol::CHANNEL_CLOSE) {
            try {
                $this->connection?->writeMethod(
                    Protocol::CLASS_CHANNEL,
                    Protocol::CHANNEL_CLOSE_OK,
                    '',
                    Connection::CHANNEL,
                );
            } catch (\Throwable) {
                // Ignore write failures while unwinding a server-initiated close.
            }

            throw Protocol::channelCloseException($method['args']);
        }
    }

    private function forgetConsumer(string $tag): void
    {
        $destination = $this->tagDestinations[$tag] ?? null;
        if ($destination === null) {
            return;
        }

        $this->logger->warning('RabbitMQ cancelled a consumer.', ['destination' => $destination]);
        unset($this->tagDestinations[$tag], $this->consumerTags[$destination], $this->buffers[$destination]);
    }

    private static function assertDestination(string $destination): void
    {
        if ($destination === '') {
            throw new \InvalidArgumentException('The RabbitMQ destination (queue name) must not be empty.');
        }

        if (strlen($destination) > self::QUEUE_NAME_MAX) {
            throw new \InvalidArgumentException(sprintf(
                'RabbitMQ queue names are limited to %d bytes, got %d.',
                self::QUEUE_NAME_MAX,
                strlen($destination),
            ));
        }
    }

    private static function boolOption(ConnectionConfig $config, string $key, bool $default): bool
    {
        $value = $config->get($key, $default);
        if (!is_bool($value)) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires [%s] to be a boolean.',
                $config->name(),
                $key,
            ));
        }

        return $value;
    }

    private static function stringOption(
        ConnectionConfig $config,
        string $key,
        string $default,
        bool $allowEmpty = false,
    ): string {
        $value = $config->get($key, $default);
        if (!is_string($value) || (!$allowEmpty && $value === '')) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires [%s] to be a non-empty string.',
                $config->name(),
                $key,
            ));
        }

        return $value;
    }

    private static function portOption(ConnectionConfig $config, bool $tls): int
    {
        $value = $config->get('port', $tls ? 5671 : 5672);
        if (is_string($value) && ctype_digit($value)) {
            $value = (int) $value;
        }
        if (!is_int($value) || $value < 1 || $value > 65535) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires [port] to be an integer between 1 and 65535.',
                $config->name(),
            ));
        }

        return $value;
    }

    private static function intOption(
        ConnectionConfig $config,
        string $key,
        int $default,
        int $min,
        int $max,
    ): int {
        $value = $config->get($key, $default);
        if (is_string($value) && ctype_digit($value)) {
            $value = (int) $value;
        }
        if (!is_int($value) || $value < $min || $value > $max) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires [%s] to be an integer between %d and %d.',
                $config->name(),
                $key,
                $min,
                $max,
            ));
        }

        return $value;
    }

    private static function floatOption(ConnectionConfig $config, string $key, float $default): float
    {
        $value = $config->get($key, $default);
        if ((!is_int($value) && !is_float($value)) || $value <= 0) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires [%s] to be a positive number of seconds.',
                $config->name(),
                $key,
            ));
        }

        return (float) $value;
    }

    /**
     * @return array<string, mixed>
     */
    private static function arrayOption(ConnectionConfig $config, string $key, array $default): array
    {
        $value = $config->get($key, $default);
        if (!is_array($value)) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires [%s] to be an array.',
                $config->name(),
                $key,
            ));
        }

        return $value;
    }
}
