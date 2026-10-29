<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\ZeroMq;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * ZeroMQ driver over ZMTP 3.1 with the NULL mechanism.
 *
 * A ZeroMQ socket has a single role, so publish() and receive() each open
 * their own TCP connection lazily: PUB/SUB or PUSH/PULL depending on [pattern].
 * ZeroMQ is brokerless and fire-and-forget: no ack, requeue, headers or keys.
 */
final class ZeroMqDriver implements DriverInterface
{
    private const BLOCK_FOREVER = 31_536_000.0;
    private const PATTERN_PUBSUB = 'pubsub';
    private const PATTERN_PUSHPULL = 'pushpull';

    private string $host;
    private int $port;
    private string $pattern;
    private ?string $identity;
    private float $timeout;
    private bool $tcpNoDelay;
    private int $bufferLimit;
    private LoggerInterface $logger;

    private ?ZmtpConnection $publisher = null;
    private ?ZmtpConnection $subscriber = null;

    /** @var array<string, true> subscribed topic prefixes */
    private array $subscriptions = [];

    /** @var array<string, list<ReceivedMessage>> destination => buffered messages */
    private array $buffer = [];

    public function __construct(ConnectionConfig $config, ?LoggerInterface $logger = null)
    {
        $this->host = $config->requireString('host');
        $this->port = self::portOption($config);
        $this->pattern = self::patternOption($config);
        $this->identity = self::nullableStringOption($config, 'identity');
        $this->timeout = self::timeoutOption($config);
        $this->tcpNoDelay = self::tcpNoDelayOption($config);
        $this->bufferLimit = self::bufferLimitOption($config);
        $this->logger = $logger ?? new NullLogger();
    }

    /**
     * @param array<string, mixed> $options
     */
    public function publish(string $destination, Message $message, array $options = []): PublishResult
    {
        $destination = self::destination($destination);

        if ($message->headers !== []) {
            throw new UnsupportedFeatureException('ZeroMQ over ZMTP/NULL does not support message headers.');
        }
        if ($message->key !== null) {
            throw new UnsupportedFeatureException('ZeroMQ over ZMTP/NULL does not support message keys.');
        }
        if ($message->delayMs !== null) {
            throw new UnsupportedFeatureException('ZeroMQ does not support delayed delivery.');
        }

        // PUB/SUB filters on the first bytes of the message, so the topic is
        // carried as a prefix. PUSH/PULL has no topics and sends the body as-is.
        $body = $this->pattern === self::PATTERN_PUBSUB ? $destination . $message->body : $message->body;
        $this->publisher()->sendMessage([$body]);

        return new PublishResult($destination);
    }

    /**
     * @param list<Message> $messages
     * @param array<string, mixed> $options
     * @return list<PublishResult>
     */
    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        throw new UnsupportedFeatureException('ZeroMQ does not support batch publishing; publish messages individually.');
    }

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage
    {
        $destination = self::destination($destination);

        if ($this->pattern === self::PATTERN_PUBSUB) {
            $this->ensureSubscribed($destination);
            $deadline = $options->timeoutMs > 0 ? microtime(true) + $options->timeoutMs / 1000 : null;

            while (true) {
                if (($this->buffer[$destination] ?? []) !== []) {
                    return array_shift($this->buffer[$destination]);
                }

                $incoming = $this->readIncoming($deadline);
                if ($incoming === null) {
                    return null;
                }

                $message = new ReceivedMessage(
                    body: $incoming['body'],
                    raw: ['destination' => $incoming['destination'], 'frames' => $incoming['frames']],
                );
                if ($incoming['destination'] === $destination) {
                    return $message;
                }

                // Bound per-destination buffering so a flood for one topic cannot exhaust memory.
                if (count($this->buffer[$incoming['destination']] ?? []) >= $this->bufferLimit) {
                    throw new ProtocolException(sprintf(
                        'ZeroMQ buffer limit of %d messages reached for destination [%s].',
                        $this->bufferLimit,
                        $incoming['destination'],
                    ));
                }

                // A message for another subscribed topic: keep it for that destination.
                $this->buffer[$incoming['destination']][] = $message;
            }
        }

        $this->subscriber();
        $deadline = $options->timeoutMs > 0 ? microtime(true) + $options->timeoutMs / 1000 : null;

        $incoming = $this->readIncoming($deadline);
        if ($incoming === null) {
            return null;
        }

        return new ReceivedMessage(body: $incoming['body'], raw: ['frames' => $incoming['frames']]);
    }

    public function capabilities(): Capabilities
    {
        return new Capabilities(
            ack: false,
            requeue: false,
            delay: false,
            consumerGroups: false,
            batchPublish: false,
            ordering: true,
            headers: false,
            // PUB/SUB only delivers to subscribers connected before the publish.
            requiresActiveSubscription: $this->pattern === self::PATTERN_PUBSUB,
        );
    }

    public function close(): void
    {
        $this->publisher?->close();
        $this->subscriber?->close();
        $this->publisher = null;
        $this->subscriber = null;
        $this->subscriptions = [];
        $this->buffer = [];
    }

    private function ensureSubscribed(string $destination): void
    {
        if (isset($this->subscriptions[$destination])) {
            return;
        }

        $this->subscriber()->sendMessage([ZmtpProtocol::subscribeBody($destination)]);
        $this->subscriptions[$destination] = true;
    }

    private function publisher(): ZmtpConnection
    {
        if ($this->publisher === null) {
            $socketType = $this->pattern === self::PATTERN_PUBSUB ? 'PUB' : 'PUSH';
            $this->publisher = $this->open($socketType);
            $this->logger->debug('Opened ZeroMQ publisher connection.', [
                'socket_type' => $socketType,
                'host' => $this->host,
                'port' => $this->port,
            ]);
        }

        return $this->publisher;
    }

    private function subscriber(): ZmtpConnection
    {
        if ($this->subscriber === null) {
            $socketType = $this->pattern === self::PATTERN_PUBSUB ? 'SUB' : 'PULL';
            $this->subscriber = $this->open($socketType);
            $this->logger->debug('Opened ZeroMQ subscriber connection.', [
                'socket_type' => $socketType,
                'host' => $this->host,
                'port' => $this->port,
            ]);
        }

        return $this->subscriber;
    }

    private function open(string $socketType): ZmtpConnection
    {
        return ZmtpConnection::open(
            $this->host,
            $this->port,
            $this->timeout,
            $this->tcpNoDelay,
            $socketType,
            $this->identity,
        );
    }

    /**
     * @return array{destination: string, body: string, frames: list<string>}|null
     */
    private function readIncoming(?float $deadline): ?array
    {
        $remaining = $deadline === null ? self::BLOCK_FOREVER : $deadline - microtime(true);
        if ($remaining <= 0) {
            return null;
        }

        $connection = $this->subscriber();
        $connection->setReadTimeout($remaining);

        try {
            $frames = $connection->receiveMessage();
        } catch (TimeoutException) {
            return null;
        }

        return $this->decodeMessage($frames);
    }

    /**
     * @param list<string> $frames
     * @return array{destination: string, body: string, frames: list<string>}
     */
    private function decodeMessage(array $frames): array
    {
        $body = implode('', $frames);

        if ($this->pattern === self::PATTERN_PUSHPULL) {
            // PUSH/PULL has no topic: every message belongs to the caller.
            return ['destination' => '', 'body' => $body, 'frames' => $frames];
        }

        $owner = null;
        foreach (array_keys($this->subscriptions) as $topic) {
            if (str_starts_with($body, $topic) && ($owner === null || strlen($topic) > strlen($owner))) {
                $owner = $topic;
            }
        }

        if ($owner === null) {
            throw new ProtocolException(sprintf(
                'Received a ZeroMQ message that matches no active subscription: [%s].',
                substr($body, 0, 64),
            ));
        }

        return [
            'destination' => $owner,
            'body' => substr($body, strlen($owner)),
            'frames' => $frames,
        ];
    }

    private static function destination(string $destination): string
    {
        if ($destination === '') {
            throw new \InvalidArgumentException('ZeroMQ destination must be a non-empty topic.');
        }

        return $destination;
    }

    private static function portOption(ConnectionConfig $config): int
    {
        $port = $config->get('port');
        if (is_string($port) && ctype_digit($port)) {
            $port = (int) $port;
        }
        if (!is_int($port) || $port < 1 || $port > 65535) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires [port] to be an integer between 1 and 65535.',
                $config->name(),
            ));
        }

        return $port;
    }

    private static function patternOption(ConnectionConfig $config): string
    {
        $pattern = $config->get('pattern', self::PATTERN_PUBSUB);
        if (!is_string($pattern) || !in_array($pattern, [self::PATTERN_PUBSUB, self::PATTERN_PUSHPULL], true)) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires [pattern] to be "pubsub" or "pushpull".',
                $config->name(),
            ));
        }

        return $pattern;
    }

    private static function timeoutOption(ConnectionConfig $config): float
    {
        $timeout = $config->get('timeout', 5.0);
        if ((!is_int($timeout) && !is_float($timeout)) || $timeout <= 0) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires [timeout] to be a positive number of seconds.',
                $config->name(),
            ));
        }

        return (float) $timeout;
    }

    private static function tcpNoDelayOption(ConnectionConfig $config): bool
    {
        // Subscription frames are tiny; Nagle would delay them behind ACKs.
        $tcpNoDelay = $config->get('tcp_nodelay', true);
        if (!is_bool($tcpNoDelay)) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires [tcp_nodelay] to be a boolean.',
                $config->name(),
            ));
        }

        return $tcpNoDelay;
    }

    private static function bufferLimitOption(ConnectionConfig $config): int
    {
        $limit = $config->get('buffer_limit', 10_000);
        if (!is_int($limit) || $limit < 1) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires [buffer_limit] to be a positive integer.',
                $config->name(),
            ));
        }

        return $limit;
    }

    private static function nullableStringOption(ConnectionConfig $config, string $key): ?string
    {
        $value = $config->get($key);
        if ($value === null) {
            return null;
        }
        if (!is_string($value) || $value === '') {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires [%s] to be a non-empty string.',
                $config->name(),
                $key,
            ));
        }

        return $value;
    }
}
