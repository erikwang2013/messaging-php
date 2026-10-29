<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Stomp;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\MessagingException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Erikwang2013\Messaging\Support\Socket;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * STOMP 1.2 driver shared by ActiveMQ Classic and ActiveMQ Artemis.
 *
 * Publishing returns a client-generated id (sent as the x-messaging-id header)
 * because STOMP does not return broker ids synchronously. The broker message id
 * is exposed as ReceivedMessage::id() for messages from other producers.
 */
final class StompDriver implements DriverInterface
{
    private const INTERNAL_HEADERS = ['x-messaging-id', 'x-messaging-types', 'x-messaging-attempts'];

    private const SYSTEM_HEADERS = [
        'destination',
        'message-id',
        'ack',
        'subscription',
        'content-length',
        'redelivered',
        'timestamp',
        'expires',
        'priority',
        'persistent',
        'receipt',
        'receipt-id',
        'transaction',
        // correlation-id is surfaced through ReceivedMessage::key().
        'correlation-id',
    ];

    private string $host;
    private int $port;
    private ?string $login;
    private ?string $passcode;
    private string $prefix;
    private float $connectTimeout;
    private Capabilities $capabilities;
    private LoggerInterface $logger;

    private ?Socket $socket = null;
    private ?FrameReader $reader = null;
    private int $generation = 0;
    private int $subscriptionSequence = 0;
    private bool $closed = false;

    /** @var array<string, string> resolved destination => subscription id */
    private array $subscriptions = [];

    /** @var array<string, string> subscription id => resolved destination */
    private array $subscriptionDestinations = [];

    /** @var array<string, list<StompFrame>> resolved destination => buffered frames */
    private array $pending = [];

    public function __construct(
        ConnectionConfig $config,
        private Preset $preset,
        ?LoggerInterface $logger = null,
    ) {
        $this->host = $config->requireString('host');
        $this->port = $this->resolvePort($config);
        $this->login = $this->resolveCredential($config, 'login');
        $this->passcode = $this->resolveCredential($config, 'passcode');

        $prefix = $config->get('prefix', '/queue/');
        if (!is_string($prefix)) {
            throw new \InvalidArgumentException('The STOMP [prefix] option must be a string.');
        }
        $this->prefix = $prefix;

        $timeout = $config->get('timeout', 5.0);
        if (!is_numeric($timeout) || (float) $timeout <= 0) {
            throw new \InvalidArgumentException('The STOMP [timeout] option must be a positive number.');
        }
        $this->connectTimeout = (float) $timeout;

        $this->capabilities = new Capabilities(
            ack: true,
            requeue: $preset->requeue,
            delay: $preset->delay,
            consumerGroups: false,
            batchPublish: false,
            ordering: false,
            headers: true,
            requiresActiveSubscription: $preset->requiresActiveSubscription,
        );
        $this->logger = $logger ?? new NullLogger();
    }

    public function publish(string $destination, Message $message, array $options = []): PublishResult
    {
        $this->assertOpen();
        $target = $this->resolveDestination($destination);
        $id = bin2hex(random_bytes(16));
        $headers = $this->publishHeaders($target, $message, $id);

        $this->ensureConnected();
        $frame = new StompFrame('SEND', $headers, $message->body);
        $this->sendFrame($frame);

        return new PublishResult($destination, $id, $frame);
    }

    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        throw new UnsupportedFeatureException(sprintf(
            'Driver [%s] does not support batch publishing.',
            $this->preset->name,
        ));
    }

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage
    {
        $this->assertOpen();
        $target = $this->resolveDestination($destination);
        $reader = $this->ensureConnected();
        $this->subscribe($target);

        if (($this->pending[$target] ?? []) !== []) {
            return $this->wrapMessage(array_shift($this->pending[$target]));
        }

        $deadline = $options->timeoutMs === 0 ? null : microtime(true) + $options->timeoutMs / 1000;

        while (true) {
            if ($deadline !== null && microtime(true) >= $deadline) {
                return null;
            }

            $remaining = $deadline === null ? null : max(0.0, $deadline - microtime(true));

            try {
                $frame = $reader->readFrame($remaining);
            } catch (ConnectionException $exception) {
                $this->disconnect();
                throw $exception;
            }

            if ($frame === null) {
                return null;
            }

            switch ($frame->command) {
                case 'MESSAGE':
                    $messageDestination = $this->messageDestination($frame);
                    if ($messageDestination === $target) {
                        return $this->wrapMessage($frame);
                    }
                    $this->pending[$messageDestination][] = $frame;
                    break;
                case 'RECEIPT':
                    break;
                case 'ERROR':
                    $this->disconnect();
                    throw $this->errorFrameException($frame);
                default:
                    throw new ProtocolException(sprintf(
                        'Unexpected STOMP frame [%s] while waiting for a message.',
                        $frame->command,
                    ));
            }
        }
    }

    public function capabilities(): Capabilities
    {
        return $this->capabilities;
    }

    public function close(): void
    {
        if ($this->socket !== null) {
            try {
                $this->sendFrame(new StompFrame('DISCONNECT'));
            } catch (\Throwable) {
                // Closing must never throw; the socket is closed below.
            }
        }

        $this->disconnect();
        $this->closed = true;
    }

    private function ensureConnected(): FrameReader
    {
        if ($this->socket !== null && $this->reader !== null) {
            return $this->reader;
        }

        $socket = Socket::connect($this->host, $this->port, $this->connectTimeout);
        $reader = new FrameReader($socket->stream());
        $this->socket = $socket;
        $this->reader = $reader;

        try {
            $headers = [
                'accept-version' => '1.2',
                'host' => $this->host,
                'heart-beat' => '0,0',
            ];
            if ($this->login !== null) {
                $headers['login'] = $this->login;
            }
            if ($this->passcode !== null) {
                $headers['passcode'] = $this->passcode;
            }

            $this->sendFrame(new StompFrame('STOMP', $headers));
            $frame = $reader->readFrame($this->connectTimeout);

            if ($frame === null) {
                throw new TimeoutException(sprintf(
                    'Timed out waiting for a CONNECTED frame from STOMP broker %s:%d.',
                    $this->host,
                    $this->port,
                ));
            }

            if ($frame->command === 'ERROR') {
                throw $this->connectionErrorException($frame);
            }

            if ($frame->command !== 'CONNECTED') {
                throw new ProtocolException(sprintf(
                    'Expected a CONNECTED frame, got [%s].',
                    $frame->command,
                ));
            }
        } catch (\Throwable $exception) {
            $this->disconnect();
            throw $exception;
        }

        $this->generation++;
        $this->logger->debug('STOMP connection established.', [
            'broker' => $this->preset->name,
            'host' => $this->host,
            'port' => $this->port,
            'generation' => $this->generation,
        ]);

        foreach ($this->subscriptions as $destination => $subscriptionId) {
            $this->sendSubscribe($destination, $subscriptionId);
        }

        return $reader;
    }

    private function disconnect(): void
    {
        $this->socket?->close();
        $this->socket = null;
        $this->reader = null;
        // Buffered frames belong to the old connection and can no longer be acked;
        // the broker redelivers unacknowledged messages after a reconnect.
        $this->pending = [];
    }

    private function sendFrame(StompFrame $frame): void
    {
        $socket = $this->socket ?? throw new ConnectionException('The STOMP connection is not established.');

        $socket->write(StompFrame::encode($frame));
    }

    private function subscribe(string $target): void
    {
        if (isset($this->subscriptions[$target])) {
            return;
        }

        $subscriptionId = 'sub-' . (++$this->subscriptionSequence);
        $this->subscriptions[$target] = $subscriptionId;
        $this->subscriptionDestinations[$subscriptionId] = $target;
        $this->sendSubscribe($target, $subscriptionId);
    }

    private function sendSubscribe(string $target, string $subscriptionId): void
    {
        $this->sendFrame(new StompFrame('SUBSCRIBE', [
            'id' => $subscriptionId,
            'destination' => $target,
            'ack' => 'client-individual',
        ]));
    }

    private function messageDestination(StompFrame $frame): string
    {
        $subscriptionId = $frame->header('subscription');
        if ($subscriptionId !== null && isset($this->subscriptionDestinations[$subscriptionId])) {
            return $this->subscriptionDestinations[$subscriptionId];
        }

        $destination = $frame->header('destination');
        if ($destination === null) {
            throw new ProtocolException('STOMP MESSAGE frame is missing the destination header.');
        }

        return $this->resolveDestination($destination);
    }

    private function wrapMessage(StompFrame $frame): ReceivedMessage
    {
        $types = $this->parseHeaderTypes($frame->header('x-messaging-types'));
        $headers = [];

        foreach ($frame->headers as $name => $value) {
            $normalized = strtolower($name);
            if (in_array($normalized, self::INTERNAL_HEADERS, true)
                || in_array($normalized, self::SYSTEM_HEADERS, true)) {
                continue;
            }

            $type = $types[$normalized] ?? null;
            $headers[$name] = $type === null ? $value : $this->castHeaderValue($value, $type);
        }

        $ackId = $frame->header('ack') ?? $frame->header('message-id');
        $generation = $this->generation;
        $redelivered = strcasecmp($frame->header('redelivered') ?? 'false', 'true') === 0;
        // Client-side requeues carry their attempt count in an internal header; broker
        // redeliveries are flagged by the broker itself.
        $attempts = $frame->header('x-messaging-attempts');
        $attempts = $attempts === null ? ($redelivered ? 2 : 1) : max(1, (int) $attempts);

        return new ReceivedMessage(
            body: $frame->body,
            headers: $headers,
            id: $frame->header('x-messaging-id') ?? $frame->header('message-id'),
            key: $frame->header('correlation-id'),
            attempts: $attempts,
            ackCallback: $ackId === null ? null : function () use ($ackId, $generation): void {
                $this->acknowledge($ackId, $generation);
            },
            rejectCallback: $ackId === null ? null : function (bool $requeue) use ($frame, $ackId, $attempts, $generation): void {
                $this->rejectMessage($frame, $ackId, $requeue, $attempts, $generation);
            },
            raw: $frame,
        );
    }

    private function acknowledge(string $ackId, int $generation): void
    {
        $this->assertCurrentGeneration($generation);
        $this->sendFrame(new StompFrame('ACK', ['id' => $ackId]));
    }

    private function rejectMessage(StompFrame $frame, string $ackId, bool $requeue, int $attempts, int $generation): void
    {
        $this->assertCurrentGeneration($generation);

        if (!$requeue) {
            $this->sendFrame(new StompFrame('ACK', ['id' => $ackId]));

            return;
        }

        if (!$this->preset->requeue) {
            throw new UnsupportedFeatureException(sprintf(
                'Driver [%s] cannot requeue rejected messages: STOMP NACK is handled like ACK and discards the '
                . 'message (ARTEMIS-1983). Close the consumer connection to trigger broker redelivery.',
                $this->preset->name,
            ));
        }

        // ActiveMQ Classic STOMP NACK is a poison ACK: non-persistent messages are silently
        // discarded by the default DLQ strategy and persistent ones go to ActiveMQ.DLQ, never
        // back to the queue. Re-send the message and ACK the original instead. A failure between
        // the two frames duplicates the message rather than losing it.
        $this->sendFrame($this->redeliveryFrame($frame, $attempts + 1));
        $this->sendFrame(new StompFrame('ACK', ['id' => $ackId]));
    }

    private function redeliveryFrame(StompFrame $frame, int $attempts): StompFrame
    {
        $destination = $frame->header('destination');
        if ($destination === null) {
            throw new ProtocolException('STOMP MESSAGE frame is missing the destination header.');
        }

        $skip = ['destination', 'message-id', 'ack', 'subscription', 'redelivered', 'content-length', 'timestamp',
            'expires', 'receipt', 'receipt-id', 'transaction'];
        $headers = ['destination' => $destination];

        foreach ($frame->headers as $name => $value) {
            if (!in_array(strtolower($name), $skip, true)) {
                $headers[$name] = $value;
            }
        }

        $headers['x-messaging-attempts'] = (string) $attempts;

        return new StompFrame('SEND', $headers, $frame->body);
    }

    private function assertCurrentGeneration(int $generation): void
    {
        $this->assertOpen();

        if ($generation !== $this->generation || $this->socket === null) {
            throw new ConnectionException(
                'The message was delivered on a previous STOMP connection and can no longer be acknowledged.',
            );
        }
    }

    /**
     * @return array<string, string>
     */
    private function publishHeaders(string $target, Message $message, string $id): array
    {
        $headers = ['destination' => $target];
        $types = [];

        foreach ($message->headers as $name => $value) {
            if (!is_string($name) || $name === '') {
                throw new \InvalidArgumentException('STOMP header names must be non-empty strings.');
            }

            if (in_array(strtolower($name), ['destination', 'content-length', 'x-messaging-id', 'x-messaging-types', 'x-messaging-attempts'], true)) {
                throw new \InvalidArgumentException(sprintf(
                    'Header [%s] is managed by the STOMP driver and cannot be set explicitly.',
                    $name,
                ));
            }

            $headers[$name] = $this->headerString($name, $value, $types);
        }

        if ($message->key !== null && $this->findHeader($headers, 'correlation-id') === null) {
            $headers['correlation-id'] = $message->key;
        }

        if ($message->delayMs !== null && $message->delayMs > 0) {
            if (!$this->preset->delay || $this->preset->delayHeader === null) {
                throw new UnsupportedFeatureException(sprintf(
                    'Driver [%s] does not support delayed messages.',
                    $this->preset->name,
                ));
            }

            $headers[$this->preset->delayHeader] = $this->preset->delayIsAbsolute
                ? (string) ((int) floor(microtime(true) * 1000) + $message->delayMs)
                : (string) $message->delayMs;
        }

        if ($types !== []) {
            $headers['x-messaging-types'] = json_encode($types, JSON_THROW_ON_ERROR);
        }

        $headers['x-messaging-id'] = $id;

        return $headers;
    }

    /**
     * @param array<string, string> $types
     */
    private function headerString(string $name, mixed $value, array &$types): string
    {
        if (is_string($value)) {
            return $value;
        }

        if (is_bool($value)) {
            $types[$name] = 'bool';

            return $value ? 'true' : 'false';
        }

        if (is_int($value)) {
            $types[$name] = 'int';

            return (string) $value;
        }

        if (is_float($value)) {
            $types[$name] = 'float';

            return (string) $value;
        }

        throw new UnsupportedFeatureException(sprintf(
            'STOMP header [%s] must be a scalar value, %s given.',
            $name,
            get_debug_type($value),
        ));
    }

    /**
     * @return array<string, string>
     */
    private function parseHeaderTypes(?string $json): array
    {
        if ($json === null || $json === '') {
            return [];
        }

        $decoded = json_decode($json, true);
        if (!is_array($decoded)) {
            return [];
        }

        $types = [];
        foreach ($decoded as $name => $type) {
            if (is_string($name) && is_string($type) && in_array($type, ['int', 'float', 'bool'], true)) {
                $types[strtolower($name)] = $type;
            }
        }

        return $types;
    }

    private function castHeaderValue(string $value, string $type): string|int|float|bool
    {
        return match ($type) {
            'int' => (int) $value,
            'float' => (float) $value,
            'bool' => $value === 'true',
            default => $value,
        };
    }

    /**
     * @param array<string, string> $headers
     */
    private function findHeader(array $headers, string $name): ?string
    {
        foreach ($headers as $key => $value) {
            if (strcasecmp($key, $name) === 0) {
                return $value;
            }
        }

        return null;
    }

    private function resolveDestination(string $destination): string
    {
        if ($destination === '') {
            throw new \InvalidArgumentException('The STOMP destination must not be empty.');
        }

        if (str_starts_with($destination, '/queue/') || str_starts_with($destination, '/topic/')) {
            return $destination;
        }

        return $this->prefix . $destination;
    }

    private function resolvePort(ConnectionConfig $config): int
    {
        $port = $config->get('port', $this->preset->defaultPort);
        if (is_string($port) && ctype_digit($port)) {
            $port = (int) $port;
        }

        if (!is_int($port) || $port < 1 || $port > 65535) {
            throw new \InvalidArgumentException('The STOMP [port] option must be an integer between 1 and 65535.');
        }

        return $port;
    }

    private function resolveCredential(ConnectionConfig $config, string $key): ?string
    {
        $value = $config->get($key, 'admin');
        if ($value !== null && !is_string($value)) {
            throw new \InvalidArgumentException(sprintf('The STOMP [%s] option must be a string or null.', $key));
        }

        return $value;
    }

    private function assertOpen(): void
    {
        if ($this->closed) {
            throw new ConnectionException('The STOMP driver has been closed.');
        }
    }

    private function connectionErrorException(StompFrame $frame): MessagingException
    {
        $message = $frame->header('message') ?? trim($frame->body);
        if ($message === '') {
            $message = 'The broker rejected the STOMP connection.';
        }

        if (stripos($message, 'version') !== false) {
            return new ProtocolException('STOMP connection failed: ' . $message);
        }

        return new AuthenticationException('STOMP connection failed: ' . $message);
    }

    private function errorFrameException(StompFrame $frame): ProtocolException
    {
        $message = $frame->header('message') ?? trim($frame->body);

        return new ProtocolException($message === '' ? 'The STOMP broker reported an error.' : 'STOMP broker error: ' . $message);
    }
}
