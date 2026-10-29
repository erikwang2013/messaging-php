<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\RocketMq;

use Erikwang2013\Messaging\Driver\RocketMq\RocketMqProtocol;
use Erikwang2013\Messaging\Exception\ProtocolException;
use PHPUnit\Framework\TestCase;

final class RocketMqProtocolTest extends TestCase
{
    public function test_request_roundtrip_encodes_the_remoting_frame(): void
    {
        $frame = RocketMqProtocol::encodeRequest(
            RocketMqProtocol::SEND_MESSAGE,
            42,
            ['topic' => 'orders', 'queueId' => '0'],
            'payload',
        );

        $length = unpack('N', substr($frame, 0, 4));
        self::assertIsArray($length);
        self::assertSame(strlen($frame) - 4, $length[1]);

        $headerLength = unpack('N', substr($frame, 4, 4));
        self::assertIsArray($headerLength);
        self::assertSame(strlen($frame) - 8 - 7, $headerLength[1]);

        $header = json_decode(substr($frame, 8, $headerLength[1]), true);
        self::assertIsArray($header);
        self::assertSame(RocketMqProtocol::SEND_MESSAGE, $header['code']);
        self::assertSame('PHP', $header['language']);
        self::assertSame(RocketMqProtocol::VERSION, $header['version']);
        self::assertSame(42, $header['opaque']);
        self::assertSame(0, $header['flag']);
        self::assertSame(['topic' => 'orders', 'queueId' => '0'], $header['extFields']);

        $command = RocketMqProtocol::decodeFrame($frame);
        self::assertSame(RocketMqProtocol::SEND_MESSAGE, $command->code());
        self::assertSame(42, $command->opaque());
        self::assertFalse($command->isResponse());
        self::assertNull($command->remark());
        self::assertSame('orders', $command->ext('topic'));
        self::assertSame(0, $command->extInt('queueId'));
        self::assertSame(7, $command->extInt('missing', 7));
        self::assertSame('payload', $command->body());
    }

    public function test_empty_ext_fields_are_omitted(): void
    {
        $frame = RocketMqProtocol::encodeRequest(RocketMqProtocol::HEART_BEAT, 1, [], '{}');
        $headerLength = unpack('N', substr($frame, 4, 4));
        self::assertIsArray($headerLength);
        $header = json_decode(substr($frame, 8, $headerLength[1]), true);

        self::assertIsArray($header);
        self::assertArrayNotHasKey('extFields', $header);
        self::assertSame('{}', RocketMqProtocol::decodeFrame($frame)->body());
    }

    public function test_response_flag_and_remark_are_decoded(): void
    {
        $header = json_encode([
            'code' => RocketMqProtocol::PULL_NOT_FOUND,
            'language' => 'JAVA',
            'version' => 407,
            'opaque' => 9,
            'flag' => 1,
            'remark' => 'no message',
            'extFields' => ['nextBeginOffset' => '3'],
        ]);
        self::assertIsString($header);

        $command = RocketMqProtocol::decodeFrame($this->frame($header));

        self::assertTrue($command->isResponse());
        self::assertSame(RocketMqProtocol::PULL_NOT_FOUND, $command->code());
        self::assertSame('no message', $command->remark());
        self::assertSame(3, $command->extInt('nextBeginOffset'));
    }

    public function test_non_json_serialize_type_is_rejected(): void
    {
        $header = '{"code":1,"opaque":1}';
        $frame = pack('NN', 4 + strlen($header), strlen($header) | (1 << 24)) . $header;

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('serialize type');

        RocketMqProtocol::decodeFrame($frame);
    }

    public function test_declared_length_must_match_the_frame(): void
    {
        $frame = pack('NN', 1000, 2) . '{}';

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('length mismatch');

        RocketMqProtocol::decodeFrame($frame);
    }

    public function test_invalid_header_json_is_rejected(): void
    {
        $this->expectException(ProtocolException::class);

        RocketMqProtocol::decodeFrame($this->frame('not-json'));
    }

    public function test_header_requires_integer_code_and_opaque(): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('[code] or [opaque]');

        RocketMqProtocol::decodeFrame($this->frame('{"code":"10","opaque":1}'));
    }

    public function test_ext_field_values_must_be_scalar(): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('[nested]');

        RocketMqProtocol::decodeFrame($this->frame('{"code":10,"opaque":1,"extFields":{"nested":{"a":1}}}'));
    }

    public function test_short_frames_are_rejected(): void
    {
        $this->expectException(ProtocolException::class);

        RocketMqProtocol::decodeFrame('abc');
    }

    private function frame(string $header, string $body = ''): string
    {
        return pack('NN', 4 + strlen($header) + strlen($body), strlen($header)) . $header . $body;
    }
}
