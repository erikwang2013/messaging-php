<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\RabbitMq;

use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\Codec;
use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\ContentHeader;
use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\FieldTable;
use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\Frame;
use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\Protocol;
use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\Reader;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use PHPUnit\Framework\TestCase;

final class AmqpProtocolTest extends TestCase
{
    public function test_protocol_header_is_amqp_0_9_1(): void
    {
        self::assertSame('414d515000000901', bin2hex(Protocol::PROTOCOL_HEADER));
    }

    public function test_heartbeat_frame_golden_bytes(): void
    {
        self::assertSame('08000000000000ce', bin2hex(Frame::encode(Frame::TYPE_HEARTBEAT, 0)));
    }

    public function test_frame_roundtrip_and_method_decoding(): void
    {
        $payload = Protocol::methodPayload(Protocol::CLASS_BASIC, Protocol::BASIC_ACK, Protocol::basicAck(5));
        $raw = Frame::encode(Frame::TYPE_METHOD, 1, $payload);

        $frame = Frame::decode($raw);
        self::assertSame(Frame::TYPE_METHOD, $frame['type']);
        self::assertSame(1, $frame['channel']);

        $method = Protocol::decodeMethod($frame['payload']);
        self::assertSame(Protocol::CLASS_BASIC, $method['class_id']);
        self::assertSame(Protocol::BASIC_ACK, $method['method_id']);
        self::assertSame(5, $method['args']['delivery_tag']);
        self::assertFalse($method['args']['multiple']);
    }

    public function test_basic_ack_and_nack_golden_bytes(): void
    {
        self::assertSame('003c0050' . '0000000000000005' . '00', bin2hex(Protocol::methodPayload(
            Protocol::CLASS_BASIC,
            Protocol::BASIC_ACK,
            Protocol::basicAck(5),
        )));

        self::assertSame('003c0078' . '0000000000000005' . '02', bin2hex(Protocol::methodPayload(
            Protocol::CLASS_BASIC,
            Protocol::BASIC_NACK,
            Protocol::basicNack(5, true),
        )));
    }

    public function test_queue_declare_golden_bytes_and_decoding(): void
    {
        $args = Protocol::queueDeclare('orders', true);

        self::assertSame('0000066f72646572730200000000', bin2hex($args));

        $method = Protocol::decodeMethod(Protocol::methodPayload(
            Protocol::CLASS_QUEUE,
            Protocol::QUEUE_DECLARE,
            $args,
        ));

        self::assertSame('orders', $method['args']['queue']);
        self::assertTrue($method['args']['durable']);
        self::assertFalse($method['args']['passive']);
        self::assertFalse($method['args']['exclusive']);
        self::assertFalse($method['args']['auto_delete']);
        self::assertFalse($method['args']['no_wait']);
        self::assertSame([], $method['args']['arguments']);
    }

    public function test_connection_start_ok_golden_bytes_and_decoding(): void
    {
        $args = Protocol::connectionStartOk([], 'PLAIN', "\x00guest\x00guest", 'en_US');

        self::assertSame(
            '00000000' . '05504c41494e' . '0000000c' . '006775657374006775657374' . '05656e5f5553',
            bin2hex($args),
        );

        $method = Protocol::decodeMethod(Protocol::methodPayload(
            Protocol::CLASS_CONNECTION,
            Protocol::CONNECTION_START_OK,
            $args,
        ));

        self::assertSame('PLAIN', $method['args']['mechanism']);
        self::assertSame("\x00guest\x00guest", $method['args']['response']);
        self::assertSame('en_US', $method['args']['locale']);
        self::assertSame([], $method['args']['client_properties']);
    }

    public function test_connection_start_decoding(): void
    {
        $payload = Codec::short(Protocol::CLASS_CONNECTION)
            . Codec::short(Protocol::CONNECTION_START)
            . "\x00\x09"
            . FieldTable::encode(['product' => 'fake-rabbit'])
            . Codec::longstr('PLAIN AMQPLAIN')
            . Codec::longstr('en_US');

        $method = Protocol::decodeMethod($payload);

        self::assertSame(0, $method['args']['version_major']);
        self::assertSame(9, $method['args']['version_minor']);
        self::assertSame(['product' => 'fake-rabbit'], $method['args']['server_properties']);
        self::assertSame('PLAIN AMQPLAIN', $method['args']['mechanisms']);
        self::assertSame('en_US', $method['args']['locales']);
    }

    public function test_field_table_simple_golden_bytes(): void
    {
        self::assertSame('000000080178530000000179', bin2hex(FieldTable::encode(['x' => 'y'])));

        $decoded = (new Reader(FieldTable::encode(['x' => 'y'])))->table();
        self::assertSame(['x' => 'y'], $decoded);
    }

    public function test_field_table_preserves_php_types(): void
    {
        $table = [
            'string' => 'abc',
            'int' => 42,
            'big' => 3000000000,
            'negative' => -7,
            'float' => 1.5,
            'true' => true,
            'false' => false,
            'null' => null,
            'list' => [1, 'two', false],
            'nested' => ['a' => 1],
        ];

        $decoded = (new Reader(FieldTable::encode($table)))->table();

        self::assertSame($table, $decoded);
    }

    public function test_field_table_rejects_non_encodable_values(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('stdClass');

        FieldTable::encode(['bad' => new \stdClass()]);
    }

    public function test_shortstr_rejects_overlong_values(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        Codec::shortstr(str_repeat('x', 256));
    }

    public function test_content_header_golden_bytes_and_decoding(): void
    {
        $header = ContentHeader::encode(5, 'm1', 'k1', ['trace' => 'abc'], 2);

        $expected = '003c'
            . '0000'
            . '0000000000000005'
            . '3480'
            . '0000000e' . '057472616365' . '53' . '00000003616263'
            . '02'
            . '026b31'
            . '026d31';

        self::assertSame($expected, bin2hex($header));

        $decoded = ContentHeader::decode($header);
        self::assertSame(5, $decoded['body_size']);
        self::assertSame(['trace' => 'abc'], $decoded['properties']['headers']);
        self::assertSame(2, $decoded['properties']['delivery_mode']);
        self::assertSame('k1', $decoded['properties']['correlation_id']);
        self::assertSame('m1', $decoded['properties']['message_id']);
    }

    public function test_content_header_without_properties(): void
    {
        $header = ContentHeader::encode(0, null, null, [], 0);

        self::assertSame('003c' . '0000' . '0000000000000000' . '0000', bin2hex($header));

        $decoded = ContentHeader::decode($header);
        self::assertSame(0, $decoded['body_size']);
        self::assertSame([], $decoded['properties']);
    }

    public function test_connection_close_error_mapping(): void
    {
        self::assertInstanceOf(
            AuthenticationException::class,
            Protocol::connectionCloseException(['reply_code' => 403, 'reply_text' => 'ACCESS_REFUSED']),
        );
        self::assertInstanceOf(
            ConnectionException::class,
            Protocol::connectionCloseException(['reply_code' => 320, 'reply_text' => 'CONNECTION_FORCED']),
        );
    }

    public function test_channel_close_error_mapping(): void
    {
        $exception = Protocol::channelCloseException([
            'reply_code' => 404,
            'reply_text' => 'NOT_FOUND - no queue',
        ]);

        self::assertStringContainsString('NOT_FOUND - no queue', $exception->getMessage());
        self::assertStringContainsString('404', $exception->getMessage());
    }

    public function test_truncated_payload_raises_protocol_exception(): void
    {
        $this->expectException(ProtocolException::class);

        (new Reader("\x05abc"))->shortstr();
    }

    public function test_malformed_frame_end_raises_protocol_exception(): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('frame-end');

        Frame::decode("\x08\x00\x00\x00\x00\x00\x00\x00");
    }

    public function test_unknown_frame_type_raises_protocol_exception(): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('frame type');

        Frame::decodeHeader("\x07\x00\x00\x00\x00\x00\x00");
    }

    public function test_unknown_method_raises_protocol_exception(): void
    {
        $this->expectException(ProtocolException::class);

        Protocol::decodeMethod(Codec::short(99) . Codec::short(1));
    }
}
