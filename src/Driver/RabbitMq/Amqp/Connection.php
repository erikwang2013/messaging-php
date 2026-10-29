<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RabbitMq\Amqp;

use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Support\Socket;
use Psr\Log\LoggerInterface;

/**
 * AMQP 0-9-1 connection: socket transport, handshake, frame I/O and
 * heartbeats. A single channel (1) is opened for all driver operations.
 */
final class Connection
{
    public const CHANNEL = 1;

    private const DEFAULT_FRAME_MAX = 131072;
    private const IDLE_SLICE = 60.0;

    private ?Socket $socket = null;
    private int $frameMax = 0;
    private int $heartbeatInterval = 0;
    private float $lastWrite = 0.0;

    public function __construct(
        private string $host,
        private int $port,
        private string $username,
        private string $password,
        private string $vhost,
        private int $heartbeat,
        private float $timeout,
        private bool $tls,
        private array $tlsContext,
        private LoggerInterface $logger,
    ) {
    }

    public function connect(): void
    {
        if ($this->socket !== null) {
            return;
        }

        $socket = Socket::connect($this->host, $this->port, $this->timeout, $this->tls, $this->tlsContext);

        try {
            $this->socket = $socket;
            $this->lastWrite = microtime(true);
            $socket->write(Protocol::PROTOCOL_HEADER);

            $start = $this->expectMethod(Protocol::CLASS_CONNECTION, Protocol::CONNECTION_START);
            $this->assertVersion($start['args']);
            $mechanisms = explode(' ', (string) ($start['args']['mechanisms'] ?? ''));
            if (!in_array('PLAIN', $mechanisms, true)) {
                throw new AuthenticationException('The RabbitMQ server does not offer SASL PLAIN authentication.');
            }

            $this->writeMethod(Protocol::CLASS_CONNECTION, Protocol::CONNECTION_START_OK, Protocol::connectionStartOk(
                [
                    'product' => 'messaging-php',
                    'version' => '1.0.0',
                    'platform' => 'PHP ' . PHP_VERSION,
                    'capabilities' => ['consumer_cancel_notify' => true],
                ],
                'PLAIN',
                "\x00" . $this->username . "\x00" . $this->password,
                'en_US',
            ));

            $tune = $this->expectMethod(Protocol::CLASS_CONNECTION, Protocol::CONNECTION_TUNE)['args'];
            $channelMax = (int) ($tune['channel_max'] ?? 0);
            $serverFrameMax = (int) ($tune['frame_max'] ?? 0);
            $serverHeartbeat = (int) ($tune['heartbeat'] ?? 0);

            $this->frameMax = $serverFrameMax > 0 ? $serverFrameMax : self::DEFAULT_FRAME_MAX;
            // RabbitMQ treats a zero on either side as "heartbeats disabled".
            $this->heartbeatInterval = ($this->heartbeat <= 0 || $serverHeartbeat <= 0)
                ? 0
                : min($this->heartbeat, $serverHeartbeat);

            $this->writeMethod(Protocol::CLASS_CONNECTION, Protocol::CONNECTION_TUNE_OK, Protocol::connectionTuneOk(
                $channelMax,
                $serverFrameMax,
                $this->heartbeatInterval,
            ));
            $this->writeMethod(Protocol::CLASS_CONNECTION, Protocol::CONNECTION_OPEN, Protocol::connectionOpen($this->vhost));
            $this->expectMethod(Protocol::CLASS_CONNECTION, Protocol::CONNECTION_OPEN_OK);

            $this->writeMethod(Protocol::CLASS_CHANNEL, Protocol::CHANNEL_OPEN, Protocol::channelOpen(), self::CHANNEL);
            $this->expectMethod(Protocol::CLASS_CHANNEL, Protocol::CHANNEL_OPEN_OK, self::CHANNEL);
        } catch (\Throwable $exception) {
            $socket->close();
            $this->socket = null;
            throw $exception;
        }

        $this->logger->debug('Connected to RabbitMQ.', [
            'host' => $this->host,
            'port' => $this->port,
            'vhost' => $this->vhost,
            'heartbeat' => $this->heartbeatInterval,
        ]);
    }

    public function isOpen(): bool
    {
        return $this->socket !== null && $this->socket->isOpen();
    }

    public function frameMax(): int
    {
        return $this->frameMax > 0 ? $this->frameMax : self::DEFAULT_FRAME_MAX;
    }

    public function writeMethod(int $classId, int $methodId, string $args = '', int $channel = 0): void
    {
        $this->writeFrame(Frame::TYPE_METHOD, $channel, Protocol::methodPayload($classId, $methodId, $args));
    }

    public function writeContent(string $headerPayload, string $body): void
    {
        $max = max(1, $this->frameMax() - (Frame::HEADER_SIZE + 1));
        if (strlen($headerPayload) > $max) {
            throw new \InvalidArgumentException(sprintf(
                'The AMQP content header of %d bytes exceeds the negotiated frame size of %d bytes; too many headers.',
                strlen($headerPayload),
                $max,
            ));
        }

        $this->writeFrame(Frame::TYPE_HEADER, self::CHANNEL, $headerPayload);

        $offset = 0;
        $length = strlen($body);
        while ($offset < $length) {
            $chunk = substr($body, $offset, $max);
            $this->writeFrame(Frame::TYPE_BODY, self::CHANNEL, $chunk);
            $offset += strlen($chunk);
        }
    }

    /**
     * Returns the next non-heartbeat frame, or null when the timeout expires.
     * A null timeout blocks until a frame arrives.
     *
     * @return array{type: int, channel: int, payload: string}|null
     */
    public function readFrame(?float $timeout): ?array
    {
        $deadline = $timeout === null ? null : microtime(true) + max(0.0, $timeout);
        $this->sendHeartbeatIfDue();

        while (true) {
            $slice = $deadline === null ? self::IDLE_SLICE : $deadline - microtime(true);
            if ($slice <= 0) {
                return null;
            }

            if ($this->heartbeatInterval > 0 && $slice > $this->heartbeatInterval) {
                $slice = (float) $this->heartbeatInterval;
            }

            if (!$this->waitForReadable($slice)) {
                $this->sendHeartbeatIfDue();
                continue;
            }

            // Bytes are available; allow the connection timeout for the rest
            // of the frame so a stalled peer fails instead of hanging forever.
            $this->setReadTimeout(max($this->timeout, 1.0));

            $frame = $this->readFrameNow();
            if ($frame['type'] === Frame::TYPE_HEARTBEAT) {
                continue;
            }

            return $frame;
        }
    }

    public function close(): void
    {
        if ($this->socket === null) {
            return;
        }

        try {
            if ($this->socket->isOpen()) {
                $this->writeMethod(Protocol::CLASS_CONNECTION, Protocol::CONNECTION_CLOSE, Protocol::connectionClose(200, 'Goodbye'));
                $this->readFrame($this->timeout);
            }
        } catch (\Throwable) {
            // Best effort: the peer may already be gone.
        }

        $this->socket->close();
        $this->socket = null;
        $this->heartbeatInterval = 0;
        $this->frameMax = 0;
    }

    /**
     * @return array{class_id: int, method_id: int, args: array<string, mixed>}
     */
    private function expectMethod(int $classId, int $methodId, int $channel = 0): array
    {
        $frame = $this->readFrame($this->timeout);
        if ($frame === null) {
            throw new TimeoutException(sprintf(
                'Timed out waiting for AMQP method %d.%d.',
                $classId,
                $methodId,
            ));
        }

        if ($frame['channel'] !== $channel || $frame['type'] !== Frame::TYPE_METHOD) {
            throw new ProtocolException(sprintf(
                'Expected an AMQP method frame on channel %d while waiting for %d.%d.',
                $channel,
                $classId,
                $methodId,
            ));
        }

        $method = Protocol::decodeMethod($frame['payload']);
        $this->throwOnCloseMethod($method, $frame['channel']);

        if ($method['class_id'] !== $classId || $method['method_id'] !== $methodId) {
            throw new ProtocolException(sprintf(
                'Expected AMQP method %d.%d, received %d.%d.',
                $classId,
                $methodId,
                $method['class_id'],
                $method['method_id'],
            ));
        }

        return $method;
    }

    /**
     * @param array{class_id: int, method_id: int, args: array<string, mixed>} $method
     */
    private function throwOnCloseMethod(array $method, int $channel): void
    {
        if ($method['class_id'] === Protocol::CLASS_CONNECTION && $method['method_id'] === Protocol::CONNECTION_CLOSE) {
            try {
                $this->writeMethod(Protocol::CLASS_CONNECTION, Protocol::CONNECTION_CLOSE_OK, '', 0);
            } catch (\Throwable) {
                // The server may close the socket immediately after sending close.
            }

            throw Protocol::connectionCloseException($method['args']);
        }

        if ($method['class_id'] === Protocol::CLASS_CHANNEL && $method['method_id'] === Protocol::CHANNEL_CLOSE) {
            try {
                $this->writeMethod(Protocol::CLASS_CHANNEL, Protocol::CHANNEL_CLOSE_OK, '', $channel);
            } catch (\Throwable) {
                // Ignore write failures while unwinding a server-initiated close.
            }

            throw Protocol::channelCloseException($method['args']);
        }
    }

    /**
     * @param array<string, mixed> $args
     */
    private function assertVersion(array $args): void
    {
        $major = (int) ($args['version_major'] ?? 0);
        $minor = (int) ($args['version_minor'] ?? 0);
        if ($major !== 0 || $minor !== 9) {
            throw new ProtocolException(sprintf(
                'RabbitMQ negotiated AMQP %d-%d, this driver speaks 0-9-1.',
                $major,
                $minor,
            ));
        }
    }

    /**
     * @return array{type: int, channel: int, payload: string}
     */
    private function readFrameNow(): array
    {
        $header = $this->socket()->read(Frame::HEADER_SIZE);

        if (strncmp($header, 'AMQP', 4) === 0) {
            $this->socket()->read(1);
            throw new ProtocolException('The server does not support AMQP 0-9-1.');
        }

        $meta = Frame::decodeHeader($header);
        if ($this->frameMax > 0 && $meta['size'] > $this->frameMax) {
            throw new ProtocolException(sprintf(
                'AMQP frame of %d bytes exceeds the negotiated frame-max of %d.',
                $meta['size'],
                $this->frameMax,
            ));
        }

        $payload = $meta['size'] > 0 ? $this->socket()->read($meta['size']) : '';
        if ($this->socket()->read(1) !== Frame::END) {
            throw new ProtocolException('Malformed AMQP frame: missing frame-end octet.');
        }

        return ['type' => $meta['type'], 'channel' => $meta['channel'], 'payload' => $payload];
    }

    private function writeFrame(int $type, int $channel, string $payload): void
    {
        $this->socket()->write(Frame::encode($type, $channel, $payload));
        $this->lastWrite = microtime(true);
    }

    private function sendHeartbeatIfDue(): void
    {
        if ($this->heartbeatInterval <= 0) {
            return;
        }

        if (microtime(true) - $this->lastWrite < $this->heartbeatInterval / 2) {
            return;
        }

        $this->writeFrame(Frame::TYPE_HEARTBEAT, 0, '');
    }

    private function socket(): Socket
    {
        return $this->socket ?? throw new ConnectionException('RabbitMQ connection is not open.');
    }

    private function waitForReadable(float $seconds): bool
    {
        $read = [$this->socket()->stream()];
        $write = null;
        $except = null;

        $whole = (int) floor($seconds);
        $microseconds = (int) round(($seconds - $whole) * 1_000_000);
        if ($microseconds >= 1_000_000) {
            $whole++;
            $microseconds = 0;
        }

        $selected = @stream_select($read, $write, $except, $whole, $microseconds);
        if ($selected === false) {
            throw new ConnectionException('Unable to poll the RabbitMQ socket.');
        }

        return $selected > 0;
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
}
