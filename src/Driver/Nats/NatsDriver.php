<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Nats;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Erikwang2013\Messaging\Support\Socket;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * NATS Core driver. At-most-once: messages are consumed on delivery, there is
 * no ack, requeue, delay or replay. JetStream is out of scope for this driver.
 */
final class NatsDriver implements DriverInterface
{
    private const BLOCK_FOREVER = 31_536_000.0;

    private string $host;
    private int $port;
    private ?string $user;
    private ?string $pass;
    private ?string $queueGroup;
    private float $timeout;
    private bool $headersEnabled;
    private LoggerInterface $logger;

    private ?Socket $socket = null;
    private bool $serverHeaders = true;

    /** @var array<string, string> subscription id => destination */
    private array $sidToDestination = [];

    /** @var array<string, string> destination => subscription id */
    private array $destinationToSid = [];

    /** @var array<string, list<ReceivedMessage>> */
    private array $buffer = [];

    private int $nextSid = 0;

    public function __construct(ConnectionConfig $config, ?LoggerInterface $logger = null)
    {
        $this->host = $config->requireString('host');
        $this->port = self::portOption($config);
        $this->timeout = self::timeoutOption($config);
        $this->headersEnabled = self::headersOption($config);
        $this->queueGroup = self::queueGroupOption($config);
        $this->user = self::nullableStringOption($config, 'user');
        $this->pass = self::nullableStringOption($config, 'pass');
        $this->logger = $logger ?? new NullLogger();

        if ($this->pass !== null && $this->user === null) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires [user] when [pass] is set.',
                $config->name(),
            ));
        }
    }

    /**
     * Opens the connection and performs the CONNECT/PING handshake.
     *
     * Called by the factory so transport and authentication problems surface
     * when the connection is created.
     */
    public function connect(): void
    {
        if ($this->socket !== null) {
            return;
        }

        $socket = Socket::connect($this->host, $this->port, $this->timeout);
        try {
            $this->socket = $socket;
            $info = $this->readInfo();
            if (array_key_exists('headers', $info)) {
                $this->serverHeaders = (bool) $info['headers'];
            } else {
                $this->serverHeaders = false;
            }

            $socket->write(NatsProtocol::connectPayload($this->user, $this->pass, $this->headersEnabled));
            $socket->write(NatsProtocol::PING);
            $this->awaitPong();
        } catch (\Throwable $exception) {
            $socket->close();
            $this->socket = null;
            throw $exception;
        }

        $this->logger->debug('Connected to NATS server.', [
            'host' => $this->host,
            'port' => $this->port,
            'headers' => $this->serverHeaders,
        ]);
    }

    /**
     * @param array<string, mixed> $options
     */
    public function publish(string $destination, Message $message, array $options = []): PublishResult
    {
        if ($message->delayMs !== null) {
            throw new UnsupportedFeatureException('NATS Core does not support delayed delivery; JetStream is required.');
        }
        if (!$this->headersEnabled && ($message->headers !== [] || $message->key !== null)) {
            throw new UnsupportedFeatureException('Headers are disabled for this NATS connection.');
        }

        $this->connect();

        if (!$this->supportsHeaders() && $message->headers !== []) {
            throw new UnsupportedFeatureException('The NATS server does not advertise header support.');
        }
        if (!$this->supportsHeaders() && $message->key !== null) {
            throw new UnsupportedFeatureException('Message keys require NATS server header support.');
        }

        $id = bin2hex(random_bytes(16));

        if ($this->supportsHeaders()) {
            $headers = $message->headers;
            $headers[NatsProtocol::ID_HEADER] = $id;
            if ($message->key !== null) {
                $headers[NatsProtocol::KEY_HEADER] = $message->key;
            }
            $frame = NatsProtocol::hpubFrame($destination, $message->body, $headers);
        } else {
            $frame = NatsProtocol::publishFrame($destination, $message->body);
        }

        $this->socket()->write($frame);
        $this->flush();

        // Without headers there is nowhere to carry the client-side id.
        return new PublishResult($destination, $this->supportsHeaders() ? $id : null);
    }

    /**
     * @param list<Message> $messages
     * @param array<string, mixed> $options
     * @return list<PublishResult>
     */
    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        throw new UnsupportedFeatureException('NATS Core does not support batch publishing; publish messages individually.');
    }

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage
    {
        $this->connect();
        $this->ensureSubscribed($destination);

        $deadline = $options->timeoutMs > 0 ? microtime(true) + $options->timeoutMs / 1000 : null;

        while (true) {
            if (($this->buffer[$destination] ?? []) !== []) {
                return array_shift($this->buffer[$destination]);
            }

            $remaining = $deadline === null ? self::BLOCK_FOREVER : $deadline - microtime(true);
            if ($remaining <= 0) {
                return null;
            }
            $this->setReadTimeout($remaining);

            try {
                $command = NatsProtocol::parseCommand($this->socket()->readLine());
            } catch (TimeoutException) {
                return null;
            }

            switch ($command['type']) {
                case 'MSG':
                case 'HMSG':
                    $frame = $this->readFrame($command);
                    $sid = $frame['sid'];
                    $owner = $this->sidToDestination[$sid] ?? null;
                    if ($owner === null) {
                        throw new ProtocolException(sprintf('Received a message for unknown subscription [%s].', $sid));
                    }

                    $message = $this->toReceivedMessage($frame);
                    if ($owner === $destination) {
                        return $message;
                    }
                    $this->buffer[$owner][] = $message;
                    break;
                case 'PING':
                    $this->socket()->write(NatsProtocol::PONG);
                    break;
                case 'PONG':
                case 'OK':
                case 'INFO':
                    break;
                case 'ERR':
                    throw NatsProtocol::errorException((string) ($command['message'] ?? ''));
                default:
                    throw new ProtocolException(sprintf('Unexpected NATS command [%s].', $command['type']));
            }
        }
    }

    public function capabilities(): Capabilities
    {
        return new Capabilities(
            ack: false,
            requeue: false,
            delay: false,
            consumerGroups: $this->queueGroup !== null,
            batchPublish: false,
            ordering: true,
            headers: $this->supportsHeaders(),
            // Core NATS is fire-and-forget: only existing subscriptions receive.
            requiresActiveSubscription: true,
        );
    }

    public function close(): void
    {
        $this->socket?->close();
        $this->socket = null;
        $this->serverHeaders = true;
        $this->buffer = [];
        $this->sidToDestination = [];
        $this->destinationToSid = [];
        $this->nextSid = 0;
    }

    /**
     * @return array<string, mixed>
     */
    private function readInfo(): array
    {
        $command = NatsProtocol::parseCommand($this->socket()->readLine());
        if ($command['type'] === 'ERR') {
            throw NatsProtocol::errorException((string) ($command['message'] ?? ''));
        }
        if ($command['type'] !== 'INFO') {
            throw new ProtocolException(sprintf('Expected INFO from NATS server, got [%s].', $command['type']));
        }

        return $command['data'] ?? [];
    }

    private function awaitPong(): void
    {
        $this->setReadTimeout($this->timeout);

        while (true) {
            $command = NatsProtocol::parseCommand($this->socket()->readLine());
            switch ($command['type']) {
                case 'PONG':
                    return;
                case 'PING':
                    $this->socket()->write(NatsProtocol::PONG);
                    break;
                case 'MSG':
                case 'HMSG':
                    $this->bufferFrame($this->readFrame($command));
                    break;
                case 'INFO':
                    if (array_key_exists('headers', $command['data'] ?? [])) {
                        $this->serverHeaders = (bool) ($command['data'] ?? [])['headers'];
                    }
                    break;
                case 'OK':
                    break;
                case 'ERR':
                    throw NatsProtocol::errorException((string) ($command['message'] ?? ''));
                default:
                    throw new ProtocolException(sprintf('Unexpected NATS command [%s].', $command['type']));
            }
        }
    }

    private function flush(): void
    {
        $this->socket()->write(NatsProtocol::PING);
        $this->awaitPong();
    }

    private function ensureSubscribed(string $destination): void
    {
        if (isset($this->destinationToSid[$destination])) {
            return;
        }

        $sid = (string) ++$this->nextSid;
        $this->destinationToSid[$destination] = $sid;
        $this->sidToDestination[$sid] = $destination;

        $this->socket()->write(NatsProtocol::subscribeFrame($destination, $this->queueGroup, $sid));
    }

    /**
     * @param array<string, mixed> $command
     * @return array{subject: string, sid: string, reply: string|null, headers: array<string, mixed>, body: string}
     */
    private function readFrame(array $command): array
    {
        $this->setReadTimeout($this->timeout);
        $socket = $this->socket();

        if (($command['type'] ?? '') === 'HMSG') {
            $headerSize = (int) ($command['headerSize'] ?? 0);
            $totalSize = (int) ($command['totalSize'] ?? 0);
            $headerBlock = $socket->read($headerSize);
            $body = $socket->read($totalSize - $headerSize);
            $headers = $headerSize > 0 ? NatsProtocol::parseHeaderBlock($headerBlock)['headers'] : [];
        } else {
            $body = $socket->read((int) ($command['size'] ?? 0));
            $headers = [];
        }

        if ($socket->read(2) !== "\r\n") {
            throw new ProtocolException('Malformed NATS message terminator.');
        }

        return [
            'subject' => (string) ($command['subject'] ?? ''),
            'sid' => (string) ($command['sid'] ?? ''),
            'reply' => isset($command['reply']) ? (string) $command['reply'] : null,
            'headers' => $headers,
            'body' => $body,
        ];
    }

    /**
     * @param array{subject: string, sid: string, reply: string|null, headers: array<string, mixed>, body: string} $frame
     */
    private function bufferFrame(array $frame): void
    {
        $owner = $this->sidToDestination[$frame['sid']] ?? null;
        if ($owner === null) {
            throw new ProtocolException(sprintf('Received a message for unknown subscription [%s].', $frame['sid']));
        }

        $this->buffer[$owner][] = $this->toReceivedMessage($frame);
    }

    /**
     * @param array{subject: string, sid: string, reply: string|null, headers: array<string, mixed>, body: string} $frame
     */
    private function toReceivedMessage(array $frame): ReceivedMessage
    {
        $headers = $frame['headers'];
        $id = $headers[NatsProtocol::ID_HEADER] ?? null;
        $key = $headers[NatsProtocol::KEY_HEADER] ?? null;
        unset($headers[NatsProtocol::ID_HEADER], $headers[NatsProtocol::KEY_HEADER]);

        return new ReceivedMessage(
            body: $frame['body'],
            headers: $headers,
            id: $id === null ? null : (string) $id,
            key: $key === null ? null : (string) $key,
            attempts: 1,
            // Core NATS is at-most-once: there is nothing to acknowledge.
            ackCallback: null,
            rejectCallback: null,
            raw: ['subject' => $frame['subject'], 'sid' => $frame['sid'], 'reply' => $frame['reply']],
        );
    }

    private function supportsHeaders(): bool
    {
        return $this->headersEnabled && $this->serverHeaders;
    }

    private function socket(): Socket
    {
        return $this->socket ?? throw new ConnectionException('NATS connection is not open.');
    }

    private function setReadTimeout(float $seconds): void
    {
        $seconds = max($seconds, 0.000001);
        $whole = (int) floor($seconds);
        $microseconds = (int) round(($seconds - $whole) * 1_000_000);
        if ($microseconds >= 1_000_000) {
            $whole++;
            $microseconds = 0;
        }

        stream_set_timeout($this->socket()->stream(), $whole, $microseconds);
    }

    private static function portOption(ConnectionConfig $config): int
    {
        $port = $config->get('port', 4222);
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

    private static function headersOption(ConnectionConfig $config): bool
    {
        $headers = $config->get('headers', true);
        if (!is_bool($headers)) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires [headers] to be a boolean.',
                $config->name(),
            ));
        }

        return $headers;
    }

    private static function queueGroupOption(ConnectionConfig $config): ?string
    {
        $queueGroup = $config->get('queue_group');
        if ($queueGroup === null) {
            return null;
        }
        if (!is_string($queueGroup) || $queueGroup === '' || preg_match('/\s/', $queueGroup) === 1) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires [queue_group] to be a non-empty token without whitespace.',
                $config->name(),
            ));
        }

        return $queueGroup;
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
