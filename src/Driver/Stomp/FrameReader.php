<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Stomp;

use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;

/**
 * Buffered STOMP frame reader. Handles heartbeat EOLs, content-length bodies
 * (binary safe) and read deadlines.
 */
final class FrameReader
{
    private const MAX_FRAME_BYTES = 64 * 1024 * 1024;
    private const BLOCKING_TIMEOUT_SECONDS = 315_360_000;

    private string $buffer = '';

    /**
     * @param resource $stream
     */
    public function __construct(private $stream)
    {
    }

    /**
     * @param float|null $timeoutSeconds null blocks indefinitely
     *
     * @return StompFrame|null null when the timeout expires between frames
     */
    public function readFrame(?float $timeoutSeconds): ?StompFrame
    {
        $deadline = $timeoutSeconds === null ? null : microtime(true) + $timeoutSeconds;

        while (true) {
            $this->discardHeartbeats();

            if ($this->buffer !== '') {
                $frame = $this->tryDecode();
                if ($frame !== null) {
                    return $frame;
                }
            }

            if ($deadline !== null && microtime(true) >= $deadline) {
                if ($this->buffer === '' || $this->buffer === "\r") {
                    return null;
                }

                throw new ConnectionException('Timed out while reading a partial STOMP frame; the connection is out of sync.');
            }

            $this->fill($deadline);
        }
    }

    private function discardHeartbeats(): void
    {
        while (true) {
            if (str_starts_with($this->buffer, "\r\n")) {
                $this->buffer = substr($this->buffer, 2);
                continue;
            }

            if (str_starts_with($this->buffer, "\n")) {
                $this->buffer = substr($this->buffer, 1);
                continue;
            }

            return;
        }
    }

    private function tryDecode(): ?StompFrame
    {
        $separator = $this->findHeaderSeparator();
        if ($separator === null) {
            $this->assertWithinLimit();

            return null;
        }

        [$position, $length] = $separator;
        $headerBlock = substr($this->buffer, 0, $position);
        $bodyStart = $position + $length;
        $contentLength = StompFrame::contentLength($headerBlock);

        if ($contentLength === null) {
            $end = strpos($this->buffer, "\x00", $bodyStart);
            if ($end === false) {
                $this->assertWithinLimit();

                return null;
            }

            $body = substr($this->buffer, $bodyStart, $end - $bodyStart);
            $this->buffer = substr($this->buffer, $end + 1);

            return StompFrame::parse($headerBlock, $body);
        }

        $end = $bodyStart + $contentLength;
        if (strlen($this->buffer) <= $end) {
            $this->assertWithinLimit();

            return null;
        }

        if ($this->buffer[$end] !== "\x00") {
            throw new ProtocolException('STOMP frame is not NUL terminated after its content-length body.');
        }

        $body = substr($this->buffer, $bodyStart, $contentLength);
        $this->buffer = substr($this->buffer, $end + 1);

        return StompFrame::parse($headerBlock, $body);
    }

    /**
     * @return array{int, int}|null header/body separator position and length
     */
    private function findHeaderSeparator(): ?array
    {
        $lf = strpos($this->buffer, "\n\n");
        $crlf = strpos($this->buffer, "\r\n\r\n");

        if ($lf !== false && ($crlf === false || $lf <= $crlf)) {
            return [$lf, 2];
        }

        if ($crlf !== false) {
            return [$crlf, 4];
        }

        return null;
    }

    private function fill(?float $deadline): void
    {
        if ($deadline === null) {
            stream_set_timeout($this->stream, self::BLOCKING_TIMEOUT_SECONDS);
        } else {
            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                return;
            }

            $seconds = (int) $remaining;
            $microseconds = max(1, (int) (($remaining - $seconds) * 1_000_000));
            stream_set_timeout($this->stream, $seconds, $microseconds);
        }

        $chunk = @fread($this->stream, 65536);
        if ($chunk !== false && $chunk !== '') {
            $this->buffer .= $chunk;

            return;
        }

        $metadata = stream_get_meta_data($this->stream);
        if ($metadata['timed_out']) {
            if ($this->buffer === '' || $this->buffer === "\r") {
                return;
            }

            throw new ConnectionException('Timed out while reading a partial STOMP frame; the connection is out of sync.');
        }

        if ($chunk === false) {
            throw new ConnectionException('Failed to read from the STOMP connection.');
        }

        throw new ConnectionException('The STOMP connection was closed by the broker.');
    }

    private function assertWithinLimit(): void
    {
        if (strlen($this->buffer) > self::MAX_FRAME_BYTES) {
            throw new ProtocolException(sprintf(
                'STOMP frame exceeds the maximum size of %d bytes.',
                self::MAX_FRAME_BYTES,
            ));
        }
    }
}
