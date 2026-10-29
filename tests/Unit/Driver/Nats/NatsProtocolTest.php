<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Nats;

use Erikwang2013\Messaging\Driver\Nats\NatsProtocol;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use PHPUnit\Framework\TestCase;

final class NatsProtocolTest extends TestCase
{
    public function test_connect_payload_without_credentials(): void
    {
        self::assertSame(
            'CONNECT {"verbose":false,"pedantic":false,"tls_required":false,"lang":"php",'
            . '"version":"1.0","protocol":1,"name":"messaging-php","headers":true}' . "\r\n",
            NatsProtocol::connectPayload(),
        );
    }

    public function test_connect_payload_omits_headers_when_disabled(): void
    {
        self::assertStringEndsNotWith(',"headers":true}' . "\r\n", NatsProtocol::connectPayload(null, null, false));
    }

    public function test_connect_payload_with_credentials(): void
    {
        $payload = NatsProtocol::connectPayload('alice', 'secret');

        self::assertStringContainsString('"user":"alice"', $payload);
        self::assertStringContainsString('"pass":"secret"', $payload);
    }

    public function test_publish_frame(): void
    {
        self::assertSame("PUB orders.created 5\r\nhello\r\n", NatsProtocol::publishFrame('orders.created', 'hello'));
    }

    public function test_publish_frame_with_reply(): void
    {
        self::assertSame(
            "PUB orders.created inbox.1 5\r\nhello\r\n",
            NatsProtocol::publishFrame('orders.created', 'hello', 'inbox.1'),
        );
    }

    public function test_publish_frame_with_zero_length_body(): void
    {
        self::assertSame("PUB orders.created 0\r\n\r\n", NatsProtocol::publishFrame('orders.created', ''));
    }

    public function test_hpub_frame_encodes_headers(): void
    {
        $block = "NATS/1.0\r\ntrace: abc\r\nattempt: 2\r\n\r\n";
        $frame = NatsProtocol::hpubFrame('orders', 'body', ['trace' => 'abc', 'attempt' => 2]);

        self::assertSame(
            'HPUB orders ' . strlen($block) . ' ' . (strlen($block) + 4) . "\r\n" . $block . "body\r\n",
            $frame,
        );
    }

    public function test_hpub_frame_with_reply(): void
    {
        $block = "NATS/1.0\r\ntrace: abc\r\n\r\n";

        self::assertSame(
            'HPUB orders inbox.9 ' . strlen($block) . ' ' . (strlen($block) + 4) . "\r\n" . $block . "body\r\n",
            NatsProtocol::hpubFrame('orders', 'body', ['trace' => 'abc'], 'inbox.9'),
        );
    }

    public function test_subscribe_frame(): void
    {
        self::assertSame("SUB orders 1\r\n", NatsProtocol::subscribeFrame('orders', null, '1'));
        self::assertSame("SUB orders workers 1\r\n", NatsProtocol::subscribeFrame('orders', 'workers', '1'));
        self::assertSame("SUB orders.* 1\r\n", NatsProtocol::subscribeFrame('orders.*', null, '1'));
    }

    public function test_parses_info(): void
    {
        $command = NatsProtocol::parseCommand("INFO {\"server_id\":\"fake\",\"headers\":true}\r\n");

        self::assertSame('INFO', $command['type']);
        self::assertTrue($command['data']['headers']);
    }

    public function test_parses_msg_without_reply(): void
    {
        $command = NatsProtocol::parseCommand("MSG orders 7 12\r\n");

        self::assertSame('MSG', $command['type']);
        self::assertSame('orders', $command['subject']);
        self::assertSame('7', $command['sid']);
        self::assertNull($command['reply']);
        self::assertSame(12, $command['size']);
    }

    public function test_parses_msg_with_reply(): void
    {
        $command = NatsProtocol::parseCommand("MSG orders 7 inbox.42 12\r\n");

        self::assertSame('inbox.42', $command['reply']);
        self::assertSame(12, $command['size']);
    }

    public function test_parses_hmsg(): void
    {
        $command = NatsProtocol::parseCommand("HMSG orders 7 20 24\r\n");

        self::assertSame('HMSG', $command['type']);
        self::assertSame(20, $command['headerSize']);
        self::assertSame(24, $command['totalSize']);
    }

    public function test_parses_hmsg_with_reply(): void
    {
        $command = NatsProtocol::parseCommand("HMSG orders 7 inbox.42 20 24\r\n");

        self::assertSame('inbox.42', $command['reply']);
        self::assertSame(20, $command['headerSize']);
        self::assertSame(24, $command['totalSize']);
    }

    public function test_parses_control_commands(): void
    {
        self::assertSame('PING', NatsProtocol::parseCommand("PING\r\n")['type']);
        self::assertSame('PONG', NatsProtocol::parseCommand("PONG\r\n")['type']);
        self::assertSame('OK', NatsProtocol::parseCommand("+OK\r\n")['type']);
    }

    public function test_parses_error_commands(): void
    {
        $command = NatsProtocol::parseCommand("-ERR 'Unknown Protocol Operation'\r\n");

        self::assertSame('ERR', $command['type']);
        self::assertSame('Unknown Protocol Operation', $command['message']);
    }

    public function test_rejects_malformed_commands(): void
    {
        $this->expectException(ProtocolException::class);

        NatsProtocol::parseCommand("BOGUS something\r\n");
    }

    public function test_rejects_malformed_msg_line(): void
    {
        $this->expectException(ProtocolException::class);

        NatsProtocol::parseCommand("MSG orders 7 twelve\r\n");
    }

    public function test_rejects_hmsg_with_header_larger_than_total(): void
    {
        $this->expectException(ProtocolException::class);

        NatsProtocol::parseCommand("HMSG orders 7 30 10\r\n");
    }

    public function test_rejects_invalid_info_json(): void
    {
        $this->expectException(ProtocolException::class);

        NatsProtocol::parseCommand("INFO not-json\r\n");
    }

    public function test_parses_header_block_and_preserves_scalar_types(): void
    {
        $parsed = NatsProtocol::parseHeaderBlock("NATS/1.0\r\ntrace: abc\r\nattempt: 2\r\nratio: 1.5\r\nflag: true\r\n\r\n");

        self::assertNull($parsed['status']);
        self::assertSame(
            ['trace' => 'abc', 'attempt' => '2', 'ratio' => '1.5', 'flag' => 'true'],
            $parsed['headers'],
        );
    }

    public function test_parses_header_status_line(): void
    {
        $parsed = NatsProtocol::parseHeaderBlock("NATS/1.0 100 Idle Heartbeat\r\n\r\n");

        self::assertSame(100, $parsed['status']);
        self::assertSame([], $parsed['headers']);
    }

    public function test_rejects_malformed_header_block(): void
    {
        $this->expectException(ProtocolException::class);

        NatsProtocol::parseHeaderBlock("HTTP/1.1\r\nA: b\r\n\r\n");
    }

    public function test_rejects_line_breaks_in_header_values(): void
    {
        $this->expectException(ProtocolException::class);

        NatsProtocol::hpubFrame('orders', 'body', ['trace' => "abc\r\nInjected: yes"]);
    }

    public function test_rejects_invalid_header_names(): void
    {
        $this->expectException(ProtocolException::class);

        NatsProtocol::hpubFrame('orders', 'body', ['bad name' => 'value']);
    }

    public function test_rejects_wildcard_publish_subject(): void
    {
        $this->expectException(ProtocolException::class);

        NatsProtocol::publishFrame('orders.*', 'body');
    }

    public function test_rejects_whitespace_subject(): void
    {
        $this->expectException(ProtocolException::class);

        NatsProtocol::publishFrame('bad subject', 'body');
    }

    public function test_maps_authorization_errors_to_authentication_exception(): void
    {
        self::assertInstanceOf(
            AuthenticationException::class,
            NatsProtocol::errorException('Authorization Violation'),
        );
        self::assertInstanceOf(
            ProtocolException::class,
            NatsProtocol::errorException('Unknown Protocol Operation'),
        );
    }
}
