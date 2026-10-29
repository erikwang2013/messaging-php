<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Stomp;

use Erikwang2013\Messaging\Driver\Stomp\StompFrame;
use Erikwang2013\Messaging\Exception\ProtocolException;
use PHPUnit\Framework\TestCase;

final class StompFrameTest extends TestCase
{
    public function test_encode_and_decode_escape_special_header_characters(): void
    {
        $frame = new StompFrame('MESSAGE', [
            'plain' => 'value',
            'colon' => 'a:b',
            'newline' => "a\nb",
            'carriage' => "a\rb",
            'backslash' => 'a\\b',
        ], 'body');

        $raw = StompFrame::encode($frame);

        self::assertStringEndsWith("\x00", $raw);
        self::assertStringContainsString('colon:a\\cb', $raw);
        self::assertStringContainsString('newline:a\\nb', $raw);
        self::assertStringContainsString('carriage:a\\rb', $raw);
        self::assertStringContainsString('backslash:a\\\\b', $raw);
        self::assertStringContainsString('content-length:4', $raw);

        $decoded = StompFrame::decode($raw);

        self::assertSame('MESSAGE', $decoded->command);
        self::assertSame($frame->headers + ['content-length' => '4'], $decoded->headers);
        self::assertSame('body', $decoded->body);
    }

    public function test_connect_frames_are_not_escaped(): void
    {
        $raw = StompFrame::encode(new StompFrame('STOMP', [
            'login' => 'a:b',
            'passcode' => 'p\\n',
        ]));

        self::assertStringContainsString("login:a:b\n", $raw);
        self::assertStringContainsString("passcode:p\\n\n", $raw);

        $decoded = StompFrame::decode($raw);

        self::assertSame('a:b', $decoded->header('login'));
        self::assertSame('p\\n', $decoded->header('passcode'));
    }

    public function test_content_length_is_added_for_binary_bodies(): void
    {
        $body = "\x00\x01\xffbinary";
        $raw = StompFrame::encode(new StompFrame('SEND', ['destination' => '/queue/x'], $body));

        self::assertStringContainsString('content-length:9', $raw);
        self::assertSame($body, StompFrame::decode($raw)->body);
    }

    public function test_empty_bodies_have_no_content_length(): void
    {
        $raw = StompFrame::encode(new StompFrame('SEND', ['destination' => '/queue/x']));

        self::assertSame("SEND\ndestination:/queue/x\n\n\x00", $raw);
    }

    public function test_crlf_frames_are_decoded(): void
    {
        $frame = StompFrame::decode("MESSAGE\r\ndestination:/queue/x\r\ncontent-length:2\r\n\r\nhi\x00");

        self::assertSame('MESSAGE', $frame->command);
        self::assertSame('/queue/x', $frame->header('destination'));
        self::assertSame('hi', $frame->body);
    }

    public function test_header_lookup_is_case_insensitive(): void
    {
        $frame = new StompFrame('MESSAGE', ['Content-Length' => '0']);

        self::assertSame('0', $frame->header('content-length'));
    }

    public function test_first_repeated_header_wins(): void
    {
        $frame = StompFrame::decode("MESSAGE\nx:first\nx:second\n\n\x00");

        self::assertSame('first', $frame->header('x'));
    }

    public function test_content_length_helper(): void
    {
        self::assertSame(12, StompFrame::contentLength("SEND\ncontent-length:12"));
        self::assertNull(StompFrame::contentLength("SEND\ndestination:/queue/x"));
    }

    public function test_content_length_helper_rejects_non_numeric_values(): void
    {
        $this->expectException(ProtocolException::class);

        StompFrame::contentLength("SEND\ncontent-length:abc");
    }

    public function test_decode_rejects_missing_nul_terminator(): void
    {
        $this->expectException(ProtocolException::class);

        StompFrame::decode("MESSAGE\n\nbody");
    }

    public function test_decode_rejects_content_length_mismatch(): void
    {
        $this->expectException(ProtocolException::class);

        StompFrame::decode("SEND\ncontent-length:5\n\nabc\x00");
    }

    public function test_decode_rejects_malformed_header_lines(): void
    {
        $this->expectException(ProtocolException::class);

        StompFrame::decode("SEND\nbroken-header\n\n\x00");
    }

    public function test_decode_rejects_invalid_escape_sequences(): void
    {
        $this->expectException(ProtocolException::class);

        StompFrame::decode("MESSAGE\nx:a\\tb\n\n\x00");
    }

    public function test_decode_rejects_dangling_escapes(): void
    {
        $this->expectException(ProtocolException::class);

        StompFrame::decode("MESSAGE\nx:abc\\\n\n\x00");
    }

    public function test_encode_rejects_invalid_header_names(): void
    {
        $this->expectException(ProtocolException::class);

        StompFrame::encode(new StompFrame('SEND', ['bad:name' => 'value']));
    }
}
