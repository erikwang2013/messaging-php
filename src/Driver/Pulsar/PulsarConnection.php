<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Pulsar;

use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Support\Socket;
use Psr\Log\LoggerInterface;

final class PulsarConnection
{
    private string $buffer = '';

    private int $requestId = 0;

    private function __construct(private Socket $socket)
    {
    }

    /**
     * Opens a connection and performs the Connect/Connected handshake.
     *
     * @param array<string, mixed> $tlsContext
     */
    public static function open(PulsarEndpoint $endpoint, float $timeout, array $tlsContext = []): self
    {
        $socket = Socket::connect(
            $endpoint->host(),
            $endpoint->port(),
            $timeout,
            $endpoint->tls(),
            $tlsContext,
        );
        $connection = new self($socket);

        try {
            $connection->send(PulsarProtocol::connectCommand());
            $frame = $connection->readFrame(microtime(true) + $timeout);

            if ($frame->type() === PulsarProtocol::COMMAND_ERROR) {
                throw PulsarErrors::exception($frame->intField(2), $frame->field(3) ?? '', 'Connect');
            }

            if ($frame->type() !== PulsarProtocol::COMMAND_CONNECTED) {
                throw new ProtocolException(sprintf(
                    'Expected Pulsar Connected, received command [%d].',
                    $frame->type(),
                ));
            }
        } catch (\Throwable $exception) {
            $connection->close();

            throw $exception;
        }

        return $connection;
    }

    /**
     * Resolves the broker that owns the topic, following lookup redirects.
     *
     * @param array<string, mixed> $tlsContext
     */
    public static function openForTopic(
        PulsarEndpoint $endpoint,
        string $topic,
        float $timeout,
        array $tlsContext = [],
        ?LoggerInterface $logger = null,
    ): self {
        $connection = self::open($endpoint, $timeout, $tlsContext);
        $authoritative = false;

        for ($attempt = 0; $attempt < 5; $attempt++) {
            $requestId = $connection->nextRequestId();

            try {
                $connection->send(PulsarProtocol::lookupCommand($topic, $requestId, $authoritative));
                $frame = $connection->readFrame(microtime(true) + $timeout);

                if ($frame->type() === PulsarProtocol::COMMAND_ERROR) {
                    throw PulsarErrors::exception(
                        $frame->intField(2),
                        $frame->field(3) ?? '',
                        sprintf('Lookup [%s]', $topic),
                    );
                }

                if ($frame->type() !== PulsarProtocol::COMMAND_LOOKUP_RESPONSE) {
                    throw new ProtocolException(sprintf(
                        'Expected Pulsar lookup response for [%s], received command [%d].',
                        $topic,
                        $frame->type(),
                    ));
                }

                if ($frame->intField(4) !== $requestId) {
                    throw new ProtocolException(sprintf('Pulsar lookup response for [%s] has a mismatched request id.', $topic));
                }

                $error = $frame->intField(6);
                if ($error !== 0) {
                    throw PulsarErrors::exception($error, $frame->field(7) ?? '', sprintf('Lookup [%s]', $topic));
                }

                $result = $frame->intField(3, -1);
                if ($result === PulsarProtocol::LOOKUP_CONNECT) {
                    $logger?->debug('Pulsar topic broker resolved.', [
                        'topic' => $topic,
                        'broker' => $endpoint->host() . ':' . $endpoint->port(),
                    ]);

                    return $connection;
                }

                if ($result !== PulsarProtocol::LOOKUP_REDIRECT) {
                    throw new ProtocolException(sprintf(
                        'Pulsar lookup for [%s] failed%s.',
                        $topic,
                        $frame->field(7) !== null ? ': ' . $frame->field(7) : '',
                    ));
                }

                $url = $endpoint->tls() ? $frame->field(2) : $frame->field(1);
                if ($url === null || $url === '') {
                    throw new ProtocolException(sprintf(
                        'Pulsar lookup redirect for [%s] did not include a usable %s service URL.',
                        $topic,
                        $endpoint->tls() ? 'TLS' : 'plain',
                    ));
                }

                $next = PulsarEndpoint::fromUrl($url);
                $authoritative = true;
                $logger?->debug('Pulsar lookup redirect.', ['topic' => $topic, 'broker' => $next->host() . ':' . $next->port()]);

                if (!$next->equals($endpoint)) {
                    $connection->close();
                    $connection = self::open($next, $timeout, $tlsContext);
                }

                $endpoint = $next;
            } catch (\Throwable $exception) {
                $connection->close();

                throw $exception;
            }
        }

        $connection->close();

        throw new ProtocolException(sprintf('Pulsar lookup for [%s] exceeded the redirect limit.', $topic));
    }

    public function send(string $command, ?string $payloadFrame = null): void
    {
        $this->socket->write(PulsarProtocol::frame($command, $payloadFrame));
    }

    public function await(int $type, float $deadline, string $context): PulsarFrame
    {
        $frame = $this->readFrame($deadline);

        if ($frame->type() === PulsarProtocol::COMMAND_ERROR) {
            throw PulsarErrors::exception($frame->intField(2), $frame->field(3) ?? '', $context);
        }

        if ($frame->type() !== $type) {
            throw new ProtocolException(sprintf(
                'Pulsar %s: expected command [%d], received [%d].',
                $context,
                $type,
                $frame->type(),
            ));
        }

        return $frame;
    }

    public function readFrame(?float $deadline): PulsarFrame
    {
        while (true) {
            $this->fill(4, $deadline);

            $header = unpack('Nsize', substr($this->buffer, 0, 4));
            if ($header === false) {
                throw new ProtocolException('Unable to unpack the Pulsar frame size.');
            }

            $totalSize = (int) $header['size'];
            if ($totalSize < 4 || $totalSize > PulsarProtocol::MAX_FRAME_SIZE) {
                throw new ProtocolException(sprintf('Invalid Pulsar frame size [%d].', $totalSize));
            }

            $this->fill(4 + $totalSize, $deadline);

            $parsed = PulsarProtocol::parseFrame($this->buffer);
            if ($parsed === null) {
                throw new ProtocolException('Unable to parse a complete Pulsar frame.');
            }

            [$frame, $consumed] = $parsed;
            $this->buffer = substr($this->buffer, $consumed);

            if ($frame->type() === PulsarProtocol::COMMAND_PING) {
                $this->send(PulsarProtocol::pongCommand());
                continue;
            }

            if ($frame->type() === PulsarProtocol::COMMAND_AUTH_CHALLENGE) {
                throw new AuthenticationException(
                    'Pulsar broker requested authentication, which this driver does not support.',
                );
            }

            return $frame;
        }
    }

    public function nextRequestId(): int
    {
        return ++$this->requestId;
    }

    public function close(): void
    {
        $this->socket->close();
    }

    private function fill(int $length, ?float $deadline): void
    {
        $stream = $this->socket->stream();

        while (strlen($this->buffer) < $length) {
            $this->applyTimeout($stream, $deadline);

            $chunk = @fread($stream, max(8192, $length - strlen($this->buffer)));
            if ($chunk === false || $chunk === '') {
                $metadata = stream_get_meta_data($stream);
                if ($metadata['timed_out']) {
                    if ($deadline === null) {
                        continue;
                    }

                    throw new TimeoutException('Pulsar read timed out.');
                }

                if ($chunk === false && !feof($stream)) {
                    throw new ConnectionException('Socket read failed.');
                }

                throw new ConnectionException('Connection closed by peer.');
            }

            $this->buffer .= $chunk;
        }
    }

    /**
     * @param resource $stream
     */
    private function applyTimeout($stream, ?float $deadline): void
    {
        if ($deadline === null) {
            stream_set_timeout($stream, 3600);

            return;
        }

        $remaining = $deadline - microtime(true);
        if ($remaining <= 0) {
            throw new TimeoutException('Pulsar read timed out.');
        }

        $seconds = (int) floor($remaining);
        $microseconds = (int) (($remaining - $seconds) * 1_000_000);
        if ($seconds === 0 && $microseconds === 0) {
            $microseconds = 1;
        }

        stream_set_timeout($stream, $seconds, $microseconds);
    }
}
