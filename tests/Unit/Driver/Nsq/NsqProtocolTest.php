<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Nsq;

use Erikwang2013\Messaging\Driver\Nsq\NsqProtocol;
use Erikwang2013\Messaging\Exception\ProtocolException;
use PHPUnit\Framework\TestCase;

final class NsqProtocolTest extends TestCase
{
    public function test_magic_is_the_four_byte_v2_handshake(): void
    {
        self::assertSame("  V2", NsqProtocol::MAGIC);
    }

    public function test_publish_command_prefixes_the_body_with_its_length(): void
    {
        self::assertSame(
            "PUB orders\n" . pack('N', 5) . 'hello',
            NsqProtocol::publishCommand('orders', 'hello'),
        );
    }

    public function test_publish_command_preserves_binary_bodies(): void
    {
        $body = "\x00\x01\xffbinary";

        self::assertSame(
            "PUB orders\n" . pack('N', strlen($body)) . $body,
            NsqProtocol::publishCommand('orders', $body),
        );
    }

    public function test_subscribe_command_includes_topic_and_channel(): void
    {
        self::assertSame("SUB orders workers\n", NsqProtocol::subscribeCommand('orders', 'workers'));
    }

    public function test_ready_finish_requeue_and_nop_commands(): void
    {
        self::assertSame("RDY 1\n", NsqProtocol::readyCommand(1));
        self::assertSame("FIN 0123456789abcdef\n", NsqProtocol::finishCommand('0123456789abcdef'));
        self::assertSame("REQ 0123456789abcdef 2500\n", NsqProtocol::requeueCommand('0123456789abcdef', 2500));
        self::assertSame("NOP\n", NsqProtocol::nopCommand());
    }

    public function test_topic_with_newline_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        NsqProtocol::publishCommand("orders\nFIN abc", 'hello');
    }

    public function test_invalid_channel_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        NsqProtocol::subscribeCommand('orders', 'bad channel');
    }

    public function test_ready_count_must_be_positive(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        NsqProtocol::readyCommand(0);
    }

    public function test_requeue_delay_must_not_be_negative(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        NsqProtocol::requeueCommand('0123456789abcdef', -1);
    }

    public function test_parse_message_extracts_timestamp_attempts_id_and_body(): void
    {
        $payload = pack('J', 1234567890123) . pack('n', 7) . '0123456789abcdef' . 'payload';

        $message = NsqProtocol::parseMessage($payload);

        self::assertSame(1234567890123, $message['timestamp']);
        self::assertSame(7, $message['attempts']);
        self::assertSame('0123456789abcdef', $message['id']);
        self::assertSame('payload', $message['body']);
    }

    public function test_parse_message_allows_empty_and_binary_bodies(): void
    {
        $header = pack('J', 1) . pack('n', 1) . '0123456789abcdef';

        self::assertSame('', NsqProtocol::parseMessage($header)['body']);
        self::assertSame("\x00\xff", NsqProtocol::parseMessage($header . "\x00\xff")['body']);
    }

    public function test_parse_message_rejects_short_payloads(): void
    {
        $this->expectException(ProtocolException::class);

        NsqProtocol::parseMessage(str_repeat('a', 25));
    }

    public function test_parse_message_rejects_non_ascii_ids(): void
    {
        $payload = pack('J', 1) . pack('n', 1) . str_repeat("\x00", 16);

        $this->expectException(ProtocolException::class);

        NsqProtocol::parseMessage($payload);
    }
}
