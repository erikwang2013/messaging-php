<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Stomp;

use Erikwang2013\Messaging\Driver\Stomp\FrameReader;
use Erikwang2013\Messaging\Driver\Stomp\StompFrame;
use Erikwang2013\Messaging\Exception\ConnectionException;
use PHPUnit\Framework\TestCase;

final class FrameReaderTest extends TestCase
{
    /** @var list<resource> */
    private array $sockets = [];

    protected function tearDown(): void
    {
        foreach ($this->sockets as $socket) {
            if (is_resource($socket)) {
                fclose($socket);
            }
        }

        $this->sockets = [];
    }

    public function test_reads_back_to_back_frames(): void
    {
        [$reader, $writer] = $this->reader();
        fwrite($writer, StompFrame::encode(new StompFrame('MESSAGE', ['message-id' => '1'], 'one')));
        fwrite($writer, StompFrame::encode(new StompFrame('MESSAGE', ['message-id' => '2'], 'two')));

        self::assertSame('one', $reader->readFrame(1.0)?->body);
        self::assertSame('two', $reader->readFrame(1.0)?->body);
    }

    public function test_skips_heartbeat_lines_between_frames(): void
    {
        [$reader, $writer] = $this->reader();
        fwrite($writer, "\n\r\n" . StompFrame::encode(new StompFrame('MESSAGE', [], 'payload')));

        $frame = $reader->readFrame(1.0);

        self::assertNotNull($frame);
        self::assertSame('payload', $frame->body);
    }

    public function test_reads_binary_bodies_with_embedded_nulls(): void
    {
        [$reader, $writer] = $this->reader();
        $body = "\x00\x01\xffbinary";
        fwrite($writer, StompFrame::encode(new StompFrame('MESSAGE', ['destination' => '/queue/x'], $body)));

        self::assertSame($body, $reader->readFrame(1.0)?->body);
    }

    public function test_reads_crlf_frames(): void
    {
        [$reader, $writer] = $this->reader();
        fwrite($writer, "MESSAGE\r\ndestination:/queue/x\r\ncontent-length:2\r\n\r\nhi\x00");

        $frame = $reader->readFrame(1.0);

        self::assertNotNull($frame);
        self::assertSame('/queue/x', $frame->header('destination'));
        self::assertSame('hi', $frame->body);
    }

    public function test_returns_null_when_the_timeout_expires_between_frames(): void
    {
        [$reader] = $this->reader();

        $start = microtime(true);
        $frame = $reader->readFrame(0.05);
        $elapsed = microtime(true) - $start;

        self::assertNull($frame);
        self::assertGreaterThanOrEqual(0.03, $elapsed);
        self::assertLessThan(2.0, $elapsed);
    }

    public function test_returns_null_after_heartbeats_only(): void
    {
        [$reader, $writer] = $this->reader();
        fwrite($writer, "\n\n\n");

        self::assertNull($reader->readFrame(0.05));
    }

    public function test_timeout_inside_a_frame_breaks_the_connection(): void
    {
        [$reader, $writer] = $this->reader();
        fwrite($writer, "MESSAGE\ndestination:/queue/x\n\npartial");

        $this->expectException(ConnectionException::class);

        $reader->readFrame(0.05);
    }

    /**
     * @return array{FrameReader, resource}
     */
    private function reader(): array
    {
        $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
        self::assertIsArray($pair);

        [$read, $write] = $pair;
        $this->sockets[] = $read;
        $this->sockets[] = $write;
        self::assertTrue(stream_set_blocking($read, true));

        return [new FrameReader($read), $write];
    }
}
