<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Support\Socket;

/**
 * One synchronous Kafka broker connection. Requests are strictly serialized so
 * correlation ids always map one response to one request.
 */
final class KafkaConnection
{
    private const MAX_FRAME_SIZE = 209_715_200;

    private int $correlationId = 0;

    private function __construct(
        private Socket $socket,
        private string $clientId,
        private float $timeout,
    ) {
    }

    /**
     * @param array<string, mixed> $tlsContext
     */
    public static function connect(
        string $host,
        int $port,
        string $clientId,
        float $timeout,
        bool $tls = false,
        array $tlsContext = [],
    ): self {
        $socket = Socket::connect($host, $port, $timeout, $tls, $tls ? $tlsContext : []);

        return new self($socket, $clientId, $timeout);
    }

    public function request(
        int $apiKey,
        int $version,
        string $body,
        int $requestHeaderVersion = 1,
        int $responseHeaderVersion = 0,
        ?float $readTimeout = null,
    ): Decoder {
        $correlationId = ++$this->correlationId;

        $header = (new Encoder())
            ->int16($apiKey)
            ->int16($version)
            ->int32($correlationId);

        if ($requestHeaderVersion >= 2) {
            $header->compactNullableString($this->clientId)->uvarint(0);
        } else {
            $header->nullableString($this->clientId);
        }

        $payload = $header->raw($body)->toString();
        $frame = (new Encoder())->int32(strlen($payload))->raw($payload)->toString();

        $this->setReadTimeout($readTimeout ?? $this->timeout);

        try {
            $this->socket->write($frame);

            $size = $this->readSize();
            if ($size < 4 || $size > self::MAX_FRAME_SIZE) {
                throw new ProtocolException(sprintf('Invalid Kafka response frame size [%d].', $size));
            }

            $response = $this->socket->read($size);
        } catch (ConnectionException $exception) {
            $this->close();

            throw $exception;
        }

        $decoder = new Decoder($response);
        $responseCorrelationId = $decoder->int32();
        if ($responseHeaderVersion >= 1) {
            $decoder->skipTags();
        }

        if ($responseCorrelationId !== $correlationId) {
            throw new ProtocolException(sprintf(
                'Kafka response correlation id [%d] does not match request [%d].',
                $responseCorrelationId,
                $correlationId,
            ));
        }

        return $decoder;
    }

    public function close(): void
    {
        $this->socket->close();
    }

    public function isOpen(): bool
    {
        return $this->socket->isOpen();
    }

    private function readSize(): int
    {
        $header = $this->socket->read(4);
        $size = unpack('N', $header);
        if ($size === false) {
            throw new ProtocolException('Unable to decode the Kafka response frame size.');
        }

        return (int) $size[1];
    }

    private function setReadTimeout(float $seconds): void
    {
        if ($seconds <= 0) {
            $seconds = 1.0;
        }

        $whole = (int) floor($seconds);
        $micro = (int) round(($seconds - $whole) * 1_000_000);
        if ($whole === 0 && $micro === 0) {
            $micro = 1;
        }

        stream_set_timeout($this->socket->stream(), $whole, $micro);
    }
}
