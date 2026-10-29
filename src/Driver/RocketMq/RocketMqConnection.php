<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RocketMq;

use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Support\Socket;

/**
 * Synchronous request/response connection over the RocketMQ remoting protocol.
 */
final class RocketMqConnection
{
    private string $buffer = '';

    private int $opaque = 0;

    private function __construct(
        private Socket $socket,
        private float $timeout,
    ) {
    }

    public static function connect(string $host, int $port, float $timeout): self
    {
        return new self(Socket::connect($host, $port, $timeout), $timeout);
    }

    /**
     * @param array<string, string> $extFields
     */
    public function request(int $code, array $extFields = [], string $body = '', ?float $deadline = null): RocketMqCommand
    {
        $opaque = $this->nextOpaque();
        $deadline ??= microtime(true) + $this->timeout;

        $this->socket->write(RocketMqProtocol::encodeRequest($code, $opaque, $extFields, $body));

        while (true) {
            $command = $this->readFrame($deadline);

            // Broker-initiated requests (e.g. consumer id notifications) are not expected in this driver.
            if (!$command->isResponse() || $command->opaque() !== $opaque) {
                continue;
            }

            return $command;
        }
    }

    public function close(): void
    {
        $this->socket->close();
    }

    private function nextOpaque(): int
    {
        $this->opaque = $this->opaque >= 0x7FFFFFFF ? 1 : $this->opaque + 1;

        return $this->opaque;
    }

    private function readFrame(float $deadline): RocketMqCommand
    {
        $this->fill(4, $deadline);

        $header = unpack('Nlength', substr($this->buffer, 0, 4));
        if ($header === false) {
            throw new ProtocolException('Unable to unpack the RocketMQ frame length.');
        }

        $length = (int) $header['length'];
        if ($length < 4 || $length > RocketMqProtocol::MAX_FRAME_SIZE) {
            throw new ProtocolException(sprintf('Invalid RocketMQ frame length [%d].', $length));
        }

        $this->fill(4 + $length, $deadline);

        $frame = substr($this->buffer, 0, 4 + $length);
        $this->buffer = substr($this->buffer, 4 + $length);

        return RocketMqProtocol::decodeFrame($frame);
    }

    private function fill(int $length, float $deadline): void
    {
        $stream = $this->socket->stream();

        while (strlen($this->buffer) < $length) {
            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                throw new TimeoutException('RocketMQ read timed out.');
            }

            $seconds = (int) floor($remaining);
            $microseconds = (int) (($remaining - $seconds) * 1_000_000);
            if ($seconds === 0 && $microseconds === 0) {
                $microseconds = 1;
            }
            stream_set_timeout($stream, $seconds, $microseconds);

            $chunk = @fread($stream, max(8192, $length - strlen($this->buffer)));
            if ($chunk === false || $chunk === '') {
                $metadata = stream_get_meta_data($stream);
                if ($metadata['timed_out']) {
                    throw new TimeoutException('RocketMQ read timed out.');
                }

                if ($chunk === false && !feof($stream)) {
                    throw new ConnectionException('Socket read failed.');
                }

                throw new ConnectionException('Connection closed by peer.');
            }

            $this->buffer .= $chunk;
        }
    }
}
