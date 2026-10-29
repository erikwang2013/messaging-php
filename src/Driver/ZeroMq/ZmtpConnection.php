<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\ZeroMq;

use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Support\Socket;

/**
 * One ZMTP 3.1 connection (NULL mechanism) with greeting/READY handshake and
 * frame multiplexing. Partial frames are buffered, so a read timeout can be
 * resumed without desynchronising the stream.
 *
 * @internal
 */
final class ZmtpConnection
{
    private Socket $socket;
    private string $readBuffer = '';

    /** @var list<string> frames of a multipart message still being read */
    private array $pendingFrames = [];

    private function __construct(Socket $socket)
    {
        $this->socket = $socket;
    }

    public static function open(
        string $host,
        int $port,
        float $timeout,
        bool $tcpNoDelay,
        string $localSocketType,
        ?string $identity = null,
    ): self {
        $context = $tcpNoDelay ? ['socket' => ['tcp_nodelay' => true]] : [];
        $socket = Socket::connect($host, $port, $timeout, false, $context);
        $connection = new self($socket);

        try {
            $connection->handshake($localSocketType, $identity);
        } catch (\Throwable $exception) {
            $socket->close();
            throw $exception;
        }

        return $connection;
    }

    private function handshake(string $localSocketType, ?string $identity): void
    {
        $this->socket->write(ZmtpProtocol::greeting());

        $greeting = $this->readExactly(ZmtpProtocol::GREETING_SIZE);
        ZmtpProtocol::validateGreeting($greeting);

        $this->socket->write(ZmtpProtocol::readyFrame($localSocketType, $identity));

        $frame = $this->readFrame();
        if (($frame['flags'] & ZmtpProtocol::FLAG_COMMAND) === 0) {
            throw new ProtocolException('Expected a ZMTP READY command after the greeting.');
        }

        $command = ZmtpProtocol::parseCommand($frame['body']);
        if ($command['name'] !== 'READY') {
            throw new ProtocolException(sprintf(
                'Expected a ZMTP READY command, got [%s].',
                $command['name'],
            ));
        }

        $peerType = $command['metadata']['Socket-Type'] ?? null;
        if ($peerType === null) {
            throw new ProtocolException('ZMTP READY is missing the Socket-Type property.');
        }

        $expected = ZmtpProtocol::compatiblePeerType($localSocketType);
        if ($peerType !== $expected) {
            throw new ProtocolException(sprintf(
                'Incompatible ZMTP socket types: local %s cannot connect to peer %s.',
                $localSocketType,
                $peerType,
            ));
        }
    }

    /**
     * @return array{flags: int, body: string}
     */
    private function readFrame(): array
    {
        $this->fill(2);
        $flags = ord($this->readBuffer[0]);
        if (($flags & ~(ZmtpProtocol::FLAG_MORE | ZmtpProtocol::FLAG_LONG | ZmtpProtocol::FLAG_COMMAND)) !== 0) {
            throw new ProtocolException(sprintf('Unsupported ZMTP frame flags [0x%02X].', $flags));
        }

        if (($flags & ZmtpProtocol::FLAG_LONG) !== 0) {
            $this->fill(10);
            $unpacked = unpack('J', substr($this->readBuffer, 1, 8));
            $size = is_array($unpacked) ? (int) $unpacked[1] : -1;
            $offset = 9;
        } else {
            $size = ord($this->readBuffer[1]);
            $offset = 2;
        }

        if ($size < 0) {
            throw new ProtocolException('Malformed ZMTP frame: invalid 64-bit size.');
        }

        $this->fill($offset + $size);
        $body = substr($this->readBuffer, $offset, $size);
        $this->readBuffer = substr($this->readBuffer, $offset + $size);

        return ['flags' => $flags, 'body' => $body];
    }

    /**
     * @param list<string> $frames
     */
    public function sendMessage(array $frames): void
    {
        $this->socket->write(ZmtpProtocol::message($frames));
    }

    /**
     * Reads one complete (possibly multipart) message, resuming a partially
     * read message after a timeout.
     *
     * @return list<string> frames
     */
    public function receiveMessage(): array
    {
        while (true) {
            $frame = $this->readFrame();
            if (($frame['flags'] & ZmtpProtocol::FLAG_COMMAND) !== 0) {
                throw new ProtocolException('Unexpected ZMTP command frame while reading a message.');
            }

            $this->pendingFrames[] = $frame['body'];
            if (($frame['flags'] & ZmtpProtocol::FLAG_MORE) === 0) {
                $frames = $this->pendingFrames;
                $this->pendingFrames = [];

                return $frames;
            }
        }
    }

    public function setReadTimeout(float $seconds): void
    {
        $seconds = max($seconds, 0.000001);
        $whole = (int) floor($seconds);
        $microseconds = (int) round(($seconds - $whole) * 1_000_000);
        if ($microseconds >= 1_000_000) {
            $whole++;
            $microseconds = 0;
        }

        stream_set_timeout($this->socket->stream(), $whole, $microseconds);
    }

    public function close(): void
    {
        $this->socket->close();
    }

    private function readExactly(int $length): string
    {
        $this->fill($length);
        $data = substr($this->readBuffer, 0, $length);
        $this->readBuffer = substr($this->readBuffer, $length);

        return $data;
    }

    private function fill(int $length): void
    {
        $stream = $this->socket->stream();

        while (strlen($this->readBuffer) < $length) {
            $chunk = @fread($stream, $length - strlen($this->readBuffer));
            if ($chunk === false || $chunk === '') {
                $metadata = stream_get_meta_data($stream);
                if ($metadata['timed_out']) {
                    throw new TimeoutException('Socket read timed out.');
                }
                if ($chunk === false || feof($stream)) {
                    throw new ConnectionException('Connection closed by peer.');
                }

                continue;
            }

            $this->readBuffer .= $chunk;
        }
    }
}
