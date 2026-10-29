<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\ZeroMq;

use Erikwang2013\Messaging\Driver\ZeroMq\ZmtpProtocol;
use Erikwang2013\Messaging\Exception\ProtocolException;
use PHPUnit\Framework\TestCase;

final class ZmtpProtocolTest extends TestCase
{
    public function test_greeting_layout(): void
    {
        $greeting = ZmtpProtocol::greeting();

        self::assertSame(64, strlen($greeting));
        self::assertSame("\xFF", $greeting[0]);
        self::assertSame(str_repeat("\x00", 8), substr($greeting, 1, 8));
        self::assertSame("\x7F", $greeting[9]);
        self::assertSame(3, ord($greeting[10]));
        self::assertSame(1, ord($greeting[11]));
        self::assertSame('NULL', rtrim(substr($greeting, 12, 20), "\x00"));
        self::assertSame("\x00", $greeting[32]);
        self::assertSame(str_repeat("\x00", 31), substr($greeting, 33));
    }

    public function test_greeting_validates_itself(): void
    {
        $this->expectNotToPerformAssertions();

        ZmtpProtocol::validateGreeting(ZmtpProtocol::greeting());
    }

    public function test_greeting_rejects_invalid_signature(): void
    {
        $greeting = ZmtpProtocol::greeting();
        $greeting[0] = "\x00";

        $this->expectException(ProtocolException::class);
        ZmtpProtocol::validateGreeting($greeting);
    }

    public function test_greeting_rejects_unsupported_version(): void
    {
        $greeting = ZmtpProtocol::greeting();
        $greeting[10] = "\x02";

        $this->expectException(ProtocolException::class);
        ZmtpProtocol::validateGreeting($greeting);
    }

    public function test_greeting_rejects_unsupported_mechanism(): void
    {
        $greeting = ZmtpProtocol::greeting();
        $greeting = substr_replace($greeting, str_pad('CURVE', 20, "\x00"), 12, 20);

        $this->expectException(ProtocolException::class);
        ZmtpProtocol::validateGreeting($greeting);
    }

    public function test_greeting_rejects_as_server(): void
    {
        $greeting = ZmtpProtocol::greeting();
        $greeting[32] = "\x01";

        $this->expectException(ProtocolException::class);
        ZmtpProtocol::validateGreeting($greeting);
    }

    public function test_greeting_rejects_wrong_length(): void
    {
        $this->expectException(ProtocolException::class);
        ZmtpProtocol::validateGreeting(substr(ZmtpProtocol::greeting(), 0, 10));
    }

    public function test_ready_frame_encodes_command_and_metadata(): void
    {
        $frame = ZmtpProtocol::readyFrame('SUB');

        self::assertSame(ZmtpProtocol::FLAG_COMMAND, ord($frame[0]));
        self::assertSame(strlen($frame) - 2, ord($frame[1]));

        $command = ZmtpProtocol::parseCommand(substr($frame, 2));
        self::assertSame('READY', $command['name']);
        self::assertSame('SUB', $command['metadata']['Socket-Type']);
        self::assertArrayNotHasKey('Identity', $command['metadata']);
    }

    public function test_ready_frame_carries_identity(): void
    {
        $frame = ZmtpProtocol::readyFrame('PUB', 'worker-1');

        $command = ZmtpProtocol::parseCommand(substr($frame, 2));
        self::assertSame('PUB', $command['metadata']['Socket-Type']);
        self::assertSame('worker-1', $command['metadata']['Identity']);
    }

    public function test_short_frame_encoding(): void
    {
        self::assertSame("\x00\x05hello", ZmtpProtocol::frame(0, 'hello'));
        self::assertSame("\x01\x01a", ZmtpProtocol::frame(ZmtpProtocol::FLAG_MORE, 'a'));
        self::assertSame("\x00\x00", ZmtpProtocol::frame(0, ''));
    }

    public function test_frames_over_255_bytes_use_long_size(): void
    {
        $body = str_repeat('x', 256);
        $frame = ZmtpProtocol::frame(0, $body);

        self::assertSame(ZmtpProtocol::FLAG_LONG, ord($frame[0]));
        self::assertSame(256, unpack('J', substr($frame, 1, 8))[1]);
        self::assertSame($body, substr($frame, 9));
    }

    public function test_multipart_message_sets_more_on_all_but_last_frame(): void
    {
        self::assertSame("\x01\x01a" . "\x00\x02bc", ZmtpProtocol::message(['a', 'bc']));
    }

    public function test_subscription_body(): void
    {
        self::assertSame("\x01orders.created", ZmtpProtocol::subscribeBody('orders.created'));
    }

    public function test_parse_command_rejects_empty_body(): void
    {
        $this->expectException(ProtocolException::class);
        ZmtpProtocol::parseCommand('');
    }

    public function test_parse_command_rejects_truncated_name(): void
    {
        $this->expectException(ProtocolException::class);
        ZmtpProtocol::parseCommand("\x05REA");
    }

    public function test_parse_metadata_rejects_truncated_value(): void
    {
        $body = "\x05READY" . "\x0BSocket-Type" . pack('N', 100) . 'PUB';

        $this->expectException(ProtocolException::class);
        ZmtpProtocol::parseCommand($body);
    }

    public function test_parse_metadata_rejects_truncated_name(): void
    {
        $body = "\x05READY" . "\x0BSocket";

        $this->expectException(ProtocolException::class);
        ZmtpProtocol::parseCommand($body);
    }

    public function test_compatible_peer_types(): void
    {
        self::assertSame('SUB', ZmtpProtocol::compatiblePeerType('PUB'));
        self::assertSame('PUB', ZmtpProtocol::compatiblePeerType('SUB'));
        self::assertSame('PULL', ZmtpProtocol::compatiblePeerType('PUSH'));
        self::assertSame('PUSH', ZmtpProtocol::compatiblePeerType('PULL'));
    }

    public function test_unknown_socket_type_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        ZmtpProtocol::compatiblePeerType('REQ');
    }
}
