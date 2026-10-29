<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Pulsar;

use Erikwang2013\Messaging\Driver\Pulsar\Protobuf;
use Erikwang2013\Messaging\Driver\Pulsar\PulsarProtocol;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class PulsarProtocolTest extends TestCase
{
    public function test_crc32c_matches_known_vectors(): void
    {
        self::assertSame(0, PulsarProtocol::checksum(''));
        self::assertSame(0xe3069283, PulsarProtocol::checksum('123456789'));
        self::assertSame(0x22620404, PulsarProtocol::checksum('The quick brown fox jumps over the lazy dog'));

        if (in_array('crc32c', hash_algos(), true)) {
            $native = unpack('Nvalue', hash('crc32c', '123456789', true));
            self::assertIsArray($native);
            self::assertSame((int) $native['value'], PulsarProtocol::checksum('123456789'));
        }
    }

    public function test_frame_layout_is_size_then_command_size_then_command_and_payload(): void
    {
        $command = Protobuf::varintField(1, PulsarProtocol::COMMAND_PING);

        self::assertSame(
            pack('N', 6) . pack('N', 2) . $command,
            PulsarProtocol::frame($command),
        );
        self::assertSame(
            pack('N', 10) . pack('N', 2) . $command . 'abcd',
            PulsarProtocol::frame($command, 'abcd'),
        );
    }

    public function test_parse_frame_returns_command_payload_and_consumed_bytes(): void
    {
        $command = Protobuf::varintField(1, PulsarProtocol::COMMAND_SEND)
            . Protobuf::bytesField(PulsarProtocol::COMMAND_SEND, Protobuf::varintField(1, 3));
        $buffer = PulsarProtocol::frame($command, 'payload') . 'trailing';

        $parsed = PulsarProtocol::parseFrame($buffer);

        self::assertNotNull($parsed);
        [$frame, $consumed] = $parsed;
        self::assertSame(PulsarProtocol::COMMAND_SEND, $frame->type());
        self::assertSame(3, $frame->intField(1));
        self::assertSame('payload', $frame->payload());
        self::assertSame(4 + 4 + strlen($command) + 7, $consumed);
        self::assertSame('trailing', substr($buffer, $consumed));
    }

    public function test_parse_frame_unwraps_the_nested_command_message(): void
    {
        $messageId = Protobuf::varintField(1, 12) . Protobuf::varintField(2, 34);
        $command = Protobuf::varintField(1, PulsarProtocol::COMMAND_SEND_RECEIPT)
            . Protobuf::bytesField(
                PulsarProtocol::COMMAND_SEND_RECEIPT,
                Protobuf::varintField(1, 7)
                . Protobuf::varintField(2, 9)
                . Protobuf::bytesField(3, $messageId),
            );

        $parsed = PulsarProtocol::parseFrame(PulsarProtocol::frame($command));

        self::assertNotNull($parsed);
        [$frame] = $parsed;
        self::assertSame(PulsarProtocol::COMMAND_SEND_RECEIPT, $frame->type());
        self::assertSame(7, $frame->intField(1));
        self::assertSame(9, $frame->intField(2));
        self::assertSame($messageId, $frame->field(3));
    }

    public function test_parse_frame_returns_null_for_incomplete_buffers(): void
    {
        $framed = PulsarProtocol::frame(Protobuf::varintField(1, PulsarProtocol::COMMAND_PING), 'body');

        self::assertNull(PulsarProtocol::parseFrame(''));
        self::assertNull(PulsarProtocol::parseFrame(substr($framed, 0, 3)));
        self::assertNull(PulsarProtocol::parseFrame(substr($framed, 0, 6)));
        self::assertNull(PulsarProtocol::parseFrame(substr($framed, 0, strlen($framed) - 1)));
    }

    public function test_parse_frame_rejects_an_invalid_total_size(): void
    {
        $this->expectException(ProtocolException::class);

        PulsarProtocol::parseFrame(pack('N', 3) . 'abc');
    }

    public function test_parse_frame_rejects_a_command_size_larger_than_the_frame(): void
    {
        $this->expectException(ProtocolException::class);

        PulsarProtocol::parseFrame(pack('N', 5) . pack('N', 4) . "\x08");
    }

    public function test_parse_frame_requires_a_command_type(): void
    {
        $this->expectException(ProtocolException::class);

        PulsarProtocol::parseFrame(PulsarProtocol::frame(Protobuf::stringField(2, 'x')));
    }

    public function test_payload_frame_roundtrip_preserves_key_headers_and_delay(): void
    {
        $before = PulsarProtocol::nowMs();
        $payloadFrame = PulsarProtocol::encodePayload(
            'producer-a',
            7,
            'hello',
            'key-1',
            ['trace' => 'abc', 'attempt' => '7'],
            $before + 1500,
        );
        $after = PulsarProtocol::nowMs();

        self::assertSame("\x0e\x01", substr($payloadFrame, 0, 2));

        $checksum = unpack('Nvalue', substr($payloadFrame, 2, 4));
        self::assertIsArray($checksum);
        self::assertSame(PulsarProtocol::checksum(substr($payloadFrame, 6)), (int) $checksum['value']);

        [$metadata, $body] = PulsarProtocol::decodePayload($payloadFrame);

        self::assertSame('hello', $body);
        self::assertSame('producer-a', Protobuf::stringValue($metadata, 1));
        self::assertSame(7, Protobuf::intValue($metadata, 2));
        self::assertGreaterThanOrEqual($before, Protobuf::intValue($metadata, 3));
        self::assertLessThanOrEqual($after, Protobuf::intValue($metadata, 3));
        self::assertSame('key-1', Protobuf::stringValue($metadata, 6));
        self::assertSame($before + 1500, Protobuf::intValue($metadata, 19));
        self::assertSame(['trace' => 'abc', 'attempt' => '7'], PulsarProtocol::decodeProperties($metadata));
        self::assertSame('key-1', PulsarProtocol::decodeKey($metadata));
    }

    public function test_payload_frame_rejects_a_checksum_mismatch(): void
    {
        $payloadFrame = PulsarProtocol::encodePayload('producer-a', 1, 'hello');
        $payloadFrame[strlen($payloadFrame) - 1] = chr(ord($payloadFrame[strlen($payloadFrame) - 1]) ^ 0x01);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('checksum');

        PulsarProtocol::decodePayload($payloadFrame);
    }

    public function test_payload_frame_rejects_an_invalid_magic(): void
    {
        $payloadFrame = PulsarProtocol::encodePayload('producer-a', 1, 'hello');
        $payloadFrame[1] = "\x02";

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('magic');

        PulsarProtocol::decodePayload($payloadFrame);
    }

    public function test_payload_frame_rejects_a_truncated_body(): void
    {
        $this->expectException(ProtocolException::class);

        PulsarProtocol::decodePayload("\x0e\x01" . pack('N', 0) . pack('N', 999));
    }

    public function test_compressed_messages_are_rejected(): void
    {
        $metadata = Protobuf::stringField(1, 'producer-a')
            . Protobuf::varintField(2, 1)
            . Protobuf::varintField(3, PulsarProtocol::nowMs())
            . Protobuf::varintField(8, 1);

        $this->expectException(UnsupportedFeatureException::class);
        $this->expectExceptionMessage('compression');

        PulsarProtocol::decodePayload(self::payloadFrame($metadata, 'hello'));
    }

    public function test_batched_messages_are_rejected(): void
    {
        $metadata = Protobuf::stringField(1, 'producer-a')
            . Protobuf::varintField(2, 1)
            . Protobuf::varintField(3, PulsarProtocol::nowMs())
            . Protobuf::boolField(11, true);

        $this->expectException(UnsupportedFeatureException::class);
        $this->expectExceptionMessage('batched');

        PulsarProtocol::decodePayload(self::payloadFrame($metadata, 'hello'));
    }

    public function test_connect_command_carries_client_and_protocol_versions(): void
    {
        $fields = Protobuf::decode(PulsarProtocol::connectCommand('test-client'));

        self::assertSame(PulsarProtocol::COMMAND_CONNECT, $fields[1][0] ?? null);
        $connect = Protobuf::decode((string) ($fields[2][0] ?? ''));
        self::assertSame('test-client', Protobuf::stringValue($connect, 1));
        self::assertSame(PulsarProtocol::PROTOCOL_VERSION, Protobuf::intValue($connect, 4));
    }

    public function test_lookup_command_carries_topic_and_authoritative_flag(): void
    {
        $fields = Protobuf::decode(PulsarProtocol::lookupCommand('persistent://public/default/events', 5, true));

        self::assertSame(PulsarProtocol::COMMAND_LOOKUP, $fields[1][0] ?? null);
        $lookup = Protobuf::decode((string) ($fields[23][0] ?? ''));
        self::assertSame('persistent://public/default/events', Protobuf::stringValue($lookup, 1));
        self::assertSame(5, Protobuf::intValue($lookup, 2));
        self::assertSame(1, Protobuf::intValue($lookup, 3));
    }

    public function test_producer_and_send_commands_carry_ids(): void
    {
        $producer = Protobuf::decode(PulsarProtocol::producerCommand('events', 11, 12, 'producer-a'));
        self::assertSame(PulsarProtocol::COMMAND_PRODUCER, $producer[1][0] ?? null);
        $producerBody = Protobuf::decode((string) ($producer[5][0] ?? ''));
        self::assertSame('events', Protobuf::stringValue($producerBody, 1));
        self::assertSame(11, Protobuf::intValue($producerBody, 2));
        self::assertSame(12, Protobuf::intValue($producerBody, 3));
        self::assertSame('producer-a', Protobuf::stringValue($producerBody, 4));

        $send = Protobuf::decode(PulsarProtocol::sendCommand(11, 9));
        self::assertSame(PulsarProtocol::COMMAND_SEND, $send[1][0] ?? null);
        $sendBody = Protobuf::decode((string) ($send[6][0] ?? ''));
        self::assertSame(11, Protobuf::intValue($sendBody, 1));
        self::assertSame(9, Protobuf::intValue($sendBody, 2));
        self::assertSame(1, Protobuf::intValue($sendBody, 3));
    }

    public function test_subscribe_command_is_shared_and_starts_earliest(): void
    {
        $fields = Protobuf::decode(PulsarProtocol::subscribeCommand('events', 'sub', 21, 22, 'consumer-a'));

        self::assertSame(PulsarProtocol::COMMAND_SUBSCRIBE, $fields[1][0] ?? null);
        $subscribe = Protobuf::decode((string) ($fields[4][0] ?? ''));
        self::assertSame('events', Protobuf::stringValue($subscribe, 1));
        self::assertSame('sub', Protobuf::stringValue($subscribe, 2));
        // Shared subscriptions honor delayed delivery; Exclusive ignores deliverAt.
        self::assertSame(PulsarProtocol::SUBSCRIPTION_SHARED, Protobuf::intValue($subscribe, 3));
        self::assertSame(21, Protobuf::intValue($subscribe, 4));
        self::assertSame(22, Protobuf::intValue($subscribe, 5));
        self::assertSame('consumer-a', Protobuf::stringValue($subscribe, 6));
        self::assertSame(PulsarProtocol::INITIAL_POSITION_EARLIEST, Protobuf::intValue($subscribe, 13));
    }

    public function test_flow_ack_and_redeliver_commands(): void
    {
        $flow = Protobuf::decode(PulsarProtocol::flowCommand(3, 5));
        self::assertSame(PulsarProtocol::COMMAND_FLOW, $flow[1][0] ?? null);
        $flowBody = Protobuf::decode((string) ($flow[11][0] ?? ''));
        self::assertSame(3, Protobuf::intValue($flowBody, 1));
        self::assertSame(5, Protobuf::intValue($flowBody, 2));

        $messageId = Protobuf::varintField(1, 12) . Protobuf::varintField(2, 34);
        $ack = Protobuf::decode(PulsarProtocol::ackCommand(3, $messageId));
        self::assertSame(PulsarProtocol::COMMAND_ACK, $ack[1][0] ?? null);
        $ackBody = Protobuf::decode((string) ($ack[10][0] ?? ''));
        self::assertSame(3, Protobuf::intValue($ackBody, 1));
        self::assertSame(PulsarProtocol::ACK_INDIVIDUAL, Protobuf::intValue($ackBody, 2));
        self::assertSame($messageId, Protobuf::stringValue($ackBody, 3));

        $redeliver = Protobuf::decode(PulsarProtocol::redeliverCommand(3));
        self::assertSame(PulsarProtocol::COMMAND_REDELIVER, $redeliver[1][0] ?? null);
        $redeliverBody = Protobuf::decode((string) ($redeliver[20][0] ?? ''));
        self::assertSame(3, Protobuf::intValue($redeliverBody, 1));
        self::assertArrayNotHasKey(2, $redeliverBody);
    }

    public function test_close_and_pong_commands(): void
    {
        $producer = Protobuf::decode(PulsarProtocol::closeProducerCommand(7, 8));
        self::assertSame(PulsarProtocol::COMMAND_CLOSE_PRODUCER, $producer[1][0] ?? null);
        $producerBody = Protobuf::decode((string) ($producer[15][0] ?? ''));
        self::assertSame(7, Protobuf::intValue($producerBody, 1));
        self::assertSame(8, Protobuf::intValue($producerBody, 2));

        $consumer = Protobuf::decode(PulsarProtocol::closeConsumerCommand(7, 9));
        self::assertSame(PulsarProtocol::COMMAND_CLOSE_CONSUMER, $consumer[1][0] ?? null);
        $consumerBody = Protobuf::decode((string) ($consumer[16][0] ?? ''));
        self::assertSame(7, Protobuf::intValue($consumerBody, 1));
        self::assertSame(9, Protobuf::intValue($consumerBody, 2));

        $pong = Protobuf::decode(PulsarProtocol::pongCommand());
        self::assertSame(PulsarProtocol::COMMAND_PONG, $pong[1][0] ?? null);
    }

    public function test_message_ids_decode_and_format(): void
    {
        $encoded = Protobuf::varintField(1, 12) . Protobuf::varintField(2, 34);

        self::assertSame(
            ['ledger_id' => 12, 'entry_id' => 34, 'partition' => -1, 'batch_index' => -1],
            PulsarProtocol::decodeMessageId($encoded),
        );
        self::assertSame('12:34', PulsarProtocol::messageIdString(PulsarProtocol::decodeMessageId($encoded)));

        $partitioned = $encoded . Protobuf::varintField(3, 2);
        self::assertSame(
            ['ledger_id' => 12, 'entry_id' => 34, 'partition' => 2, 'batch_index' => -1],
            PulsarProtocol::decodeMessageId($partitioned),
        );
        self::assertSame('12:34:2', PulsarProtocol::messageIdString(PulsarProtocol::decodeMessageId($partitioned)));

        $batched = $partitioned . Protobuf::varintField(4, 1);
        self::assertSame(1, PulsarProtocol::decodeMessageId($batched)['batch_index']);
    }

    public function test_message_id_requires_ledger_and_entry(): void
    {
        $this->expectException(ProtocolException::class);

        PulsarProtocol::decodeMessageId(Protobuf::varintField(1, 12));
    }

    public function test_properties_and_base64_keys_are_decoded(): void
    {
        $property = Protobuf::stringField(1, 'trace') . Protobuf::stringField(2, 'abc');

        self::assertSame(['trace' => 'abc'], PulsarProtocol::decodeProperties([4 => [$property]]));

        $metadata = Protobuf::stringField(6, base64_encode("\x00key")) . Protobuf::boolField(17, true);
        self::assertSame("\x00key", PulsarProtocol::decodeKey(Protobuf::decode($metadata)));
    }

    public function test_malformed_properties_are_rejected(): void
    {
        $this->expectException(ProtocolException::class);

        PulsarProtocol::decodeProperties([4 => [Protobuf::stringField(1, 'trace')]]);
    }

    private static function payloadFrame(string $metadata, string $body): string
    {
        $checksumInput = pack('N', strlen($metadata)) . $metadata . $body;

        return "\x0e\x01" . pack('N', PulsarProtocol::checksum($checksumInput)) . $checksumInput;
    }
}
