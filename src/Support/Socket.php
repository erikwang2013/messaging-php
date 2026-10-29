<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Support;

use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\TimeoutException;

final class Socket
{
    /**
     * @param resource $stream
     */
    private function __construct(private $stream)
    {
    }

    /**
     * @param array<string, mixed> $context
     */
    public static function connect(
        string $host,
        int $port,
        float $timeout = 5.0,
        bool $tls = false,
        array $context = [],
    ): self {
        $address = sprintf('%s://%s:%d', $tls ? 'tls' : 'tcp', $host, $port);
        $stream = @stream_socket_client(
            $address,
            $errorCode,
            $errorMessage,
            $timeout,
            STREAM_CLIENT_CONNECT,
            stream_context_create($context),
        );

        if ($stream === false) {
            throw new ConnectionException(sprintf(
                'Unable to connect to %s: %s (%d).',
                $address,
                $errorMessage !== '' ? $errorMessage : 'unknown error',
                $errorCode,
            ));
        }

        stream_set_timeout($stream, (int) $timeout, (int) (($timeout - floor($timeout)) * 1_000_000));
        stream_set_blocking($stream, true);

        return new self($stream);
    }

    public function write(string $data): void
    {
        $length = strlen($data);
        $written = 0;

        while ($written < $length) {
            $chunk = @fwrite($this->stream, substr($data, $written));
            if ($chunk === false || $chunk === 0) {
                throw new ConnectionException('Socket write failed.');
            }

            $written += $chunk;
        }
    }

    public function read(int $length): string
    {
        if ($length < 0) {
            throw new \InvalidArgumentException('Read length must be zero or positive.');
        }

        $buffer = '';
        while (strlen($buffer) < $length) {
            $chunk = @fread($this->stream, $length - strlen($buffer));
            if ($chunk === false) {
                throw new ConnectionException('Socket read failed.');
            }

            if ($chunk === '') {
                $this->throwReadFailure();
            }

            $buffer .= $chunk;
        }

        return $buffer;
    }

    public function readLine(int $maxLength = 65536): string
    {
        $line = @fgets($this->stream, $maxLength);
        if ($line === false) {
            $this->throwReadFailure();
        }

        return $line;
    }

    public function close(): void
    {
        if (is_resource($this->stream)) {
            fclose($this->stream);
        }
    }

    public function isOpen(): bool
    {
        return is_resource($this->stream) && !feof($this->stream);
    }

    /**
     * @return resource
     */
    public function stream()
    {
        return $this->stream;
    }

    private function throwReadFailure(): never
    {
        $metadata = stream_get_meta_data($this->stream);
        if ($metadata['timed_out']) {
            throw new TimeoutException('Socket read timed out.');
        }

        throw new ConnectionException('Connection closed by peer.');
    }
}
