<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Nsq;

use Erikwang2013\Messaging\Driver\Nsq\NsqFrame;
use Erikwang2013\Messaging\Driver\Nsq\NsqProtocol;
use PHPUnit\Framework\TestCase;

final class NsqFrameTest extends TestCase
{
    public function test_response_frame(): void
    {
        $frame = new NsqFrame(NsqProtocol::FRAME_RESPONSE, 'OK');

        self::assertTrue($frame->isResponse());
        self::assertFalse($frame->isHeartbeat());
        self::assertFalse($frame->isError());
        self::assertFalse($frame->isMessage());
    }

    public function test_heartbeat_frame(): void
    {
        $frame = new NsqFrame(NsqProtocol::FRAME_RESPONSE, '_heartbeat_');

        self::assertTrue($frame->isResponse());
        self::assertTrue($frame->isHeartbeat());
    }

    public function test_error_frame_strips_trailing_newline(): void
    {
        $frame = new NsqFrame(NsqProtocol::FRAME_ERROR, "E_INVALID bad request\n");

        self::assertTrue($frame->isError());
        self::assertSame('E_INVALID bad request', $frame->errorText());
    }

    public function test_message_frame_delegates_to_protocol_parser(): void
    {
        $payload = pack('J', 42) . pack('n', 2) . '0123456789abcdef' . 'hello';

        $frame = new NsqFrame(NsqProtocol::FRAME_MESSAGE, $payload);

        self::assertTrue($frame->isMessage());
        self::assertSame([
            'timestamp' => 42,
            'attempts' => 2,
            'id' => '0123456789abcdef',
            'body' => 'hello',
        ], $frame->message());
    }
}
