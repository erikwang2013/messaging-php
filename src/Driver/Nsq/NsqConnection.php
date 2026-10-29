<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Nsq;

use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Support\Socket;

final class NsqConnection
{
    private const MAX_FRAME_SIZE = 64 * 1024 * 1024;

    private string $buffer = '';

    private function __construct(
        private readonly Socket $socket,
        private readonly float $readTimeout,
    ) {
    }

    public static function connect(string $host, int $port, float $timeout): self
    {
        $socket = Socket::connect($host, $port, $timeout);
        $socket->write(NsqProtocol::MAGIC);

        return new self($socket, $timeout);
    }

    public function publish(string $topic, string $body): void
    {
        $this->socket->write(NsqProtocol::publishCommand($topic, $body));
        $this->awaitResponse();
    }

    public function subscribe(string $topic, string $channel): void
    {
        $this->socket->write(NsqProtocol::subscribeCommand($topic, $channel));
        $this->awaitResponse();
    }

    /**
     * @return array{timestamp: int, attempts: int, id: string, body: string}
     */
    public function receiveMessage(?float $deadline): array
    {
        $this->socket->write(NsqProtocol::readyCommand(1));

        while (true) {
            $frame = $this->readFrame($deadline);

            if ($frame->isHeartbeat()) {
                $this->socket->write(NsqProtocol::nopCommand());
                continue;
            }

            if ($frame->isError()) {
                throw new ProtocolException('NSQ error: ' . $frame->errorText());
            }

            if ($frame->isMessage()) {
                return $frame->message();
            }
        }
    }

    public function finish(string $id): void
    {
        $this->socket->write(NsqProtocol::finishCommand($id));
    }

    public function requeue(string $id, int $delayMs): void
    {
        $this->socket->write(NsqProtocol::requeueCommand($id, $delayMs));
    }

    public function close(): void
    {
        $this->socket->close();
    }

    private function awaitResponse(): void
    {
        $deadline = microtime(true) + $this->readTimeout;

        while (true) {
            $frame = $this->readFrame($deadline);

            if ($frame->isHeartbeat()) {
                $this->socket->write(NsqProtocol::nopCommand());
                continue;
            }

            if ($frame->isError()) {
                throw new ProtocolException('NSQ error: ' . $frame->errorText());
            }

            if ($frame->isResponse() && $frame->payload === 'OK') {
                return;
            }

            throw new ProtocolException(sprintf('Unexpected NSQ response [%s].', $frame->payload));
        }
    }

    private function readFrame(?float $deadline): NsqFrame
    {
        $this->fill(4, $deadline);

        $header = unpack('Nsize', substr($this->buffer, 0, 4));
        if ($header === false) {
            throw new ProtocolException('Unable to unpack the NSQ frame size.');
        }

        $size = (int) $header['size'];
        if ($size < 4 || $size > self::MAX_FRAME_SIZE) {
            throw new ProtocolException(sprintf('Invalid NSQ frame size [%d].', $size));
        }

        $this->fill(4 + $size, $deadline);

        $type = unpack('Ntype', substr($this->buffer, 4, 4));
        if ($type === false) {
            throw new ProtocolException('Unable to unpack the NSQ frame type.');
        }

        $frame = new NsqFrame((int) $type['type'], substr($this->buffer, 8, $size - 4));
        $this->buffer = substr($this->buffer, 4 + $size);

        return $frame;
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

                    throw new TimeoutException('NSQ read timed out.');
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
            throw new TimeoutException('NSQ read timed out.');
        }

        $seconds = (int) floor($remaining);
        $microseconds = (int) (($remaining - $seconds) * 1_000_000);
        if ($seconds === 0 && $microseconds === 0) {
            $microseconds = 1;
        }

        stream_set_timeout($stream, $seconds, $microseconds);
    }
}
