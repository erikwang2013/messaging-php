<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\RocketMq;

use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Driver\RocketMq\RocketMqMessageCodec;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class RocketMqMessageCodecTest extends TestCase
{
    public function test_properties_carry_headers_key_and_delay(): void
    {
        $raw = RocketMqMessageCodec::buildProperties(
            new Message('payload', ['trace' => 'abc', 'attempt' => 2, 'flag' => true], 'key-1', 2500),
            'uniq-1',
        );

        $properties = RocketMqMessageCodec::decodeProperties($raw);

        self::assertSame('abc', $properties['trace']);
        self::assertSame('2', $properties['attempt']);
        self::assertSame('true', $properties['flag']);
        self::assertSame('key-1', $properties['KEYS']);
        self::assertSame('uniq-1', $properties['UNIQ_KEY']);
        self::assertSame('2', $properties['DELAY']);
        self::assertSame('true', $properties['WAIT']);
    }

    public function test_reserved_header_names_are_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('reserved');

        RocketMqMessageCodec::buildProperties(new Message('payload', ['UNIQ_KEY' => 'x']), 'uniq-1');
    }

    public function test_non_scalar_header_values_are_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        RocketMqMessageCodec::buildProperties(new Message('payload', ['nested' => ['a']]), 'uniq-1');
    }

    public function test_property_separators_are_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        RocketMqMessageCodec::encodeProperties(['trace' => "a\x02b"]);
    }

    public function test_properties_ignore_malformed_segments(): void
    {
        self::assertSame(
            ['good' => 'value'],
            RocketMqMessageCodec::decodeProperties("broken\x02good\x01value\x02empty\x01\x02\x01no-key"),
        );
    }

    public function test_system_properties_are_hidden_from_user_headers(): void
    {
        self::assertSame(
            ['trace' => 'abc'],
            RocketMqMessageCodec::userProperties([
                'trace' => 'abc',
                'KEYS' => 'key-1',
                'UNIQ_KEY' => 'id',
                'RETRY_TOPIC' => 'orders',
            ]),
        );
    }

    /**
     * @return array<string, array{0: int, 1: int}>
     */
    public static function delayLevels(): array
    {
        return [
            'immediate' => [0, 0],
            '1s' => [1000, 1],
            'just over 1s' => [1001, 2],
            '5s' => [5000, 2],
            '1m' => [60000, 5],
            '90s' => [90000, 6],
            '2h' => [7200000, 18],
        ];
    }

    /**
     * @dataProvider delayLevels
     */
    public function test_delay_levels_use_the_brokers_default_schedule(int $delayMs, int $level): void
    {
        self::assertSame($level, RocketMqMessageCodec::delayLevel($delayMs));
    }

    public function test_delay_beyond_two_hours_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        RocketMqMessageCodec::delayLevel(7200001);
    }

    public function test_stored_message_record_is_decoded(): void
    {
        $properties = RocketMqMessageCodec::encodeProperties([
            'UNIQ_KEY' => 'uniq-1',
            'KEYS' => 'key-1',
            'RETRY_TOPIC' => 'orders',
        ]);

        $record = $this->record('hello', 'orders', $properties, 12, 2, 77);
        $messages = RocketMqMessageCodec::decodeMessages($record);

        self::assertCount(1, $messages);
        $message = $messages[0];
        self::assertSame('hello', $message['body']);
        self::assertSame('orders', $message['topic']);
        self::assertSame(0, $message['queueId']);
        self::assertSame(12, $message['queueOffset']);
        self::assertSame(77, $message['physicalOffset']);
        self::assertSame(2, $message['reconsumeTimes']);
        self::assertSame('uniq-1', $message['properties']['UNIQ_KEY']);
        self::assertSame('orders', $message['properties']['RETRY_TOPIC']);
    }

    public function test_concatenated_records_are_decoded(): void
    {
        $messages = RocketMqMessageCodec::decodeMessages(
            $this->record('one', 'orders', '') . $this->record('two', 'orders', ''),
        );

        self::assertCount(2, $messages);
        self::assertSame('one', $messages[0]['body']);
        self::assertSame('two', $messages[1]['body']);
    }

    public function test_ipv6_hosts_change_the_record_layout(): void
    {
        $record = $this->record(
            'ipv6',
            'orders',
            '',
            0,
            0,
            0,
            RocketMqMessageCodec::BORNHOST_V6_FLAG | RocketMqMessageCodec::STOREHOST_V6_FLAG,
        );

        self::assertSame('ipv6', RocketMqMessageCodec::decodeMessages($record)[0]['body']);
    }

    public function test_invalid_magic_is_rejected(): void
    {
        $record = $this->record('hello', 'orders', '');
        $record = substr_replace($record, pack('N', 0), 4, 4);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('magic');

        RocketMqMessageCodec::decodeMessages($record);
    }

    public function test_truncated_record_is_rejected(): void
    {
        $record = $this->record('hello', 'orders', '');

        $this->expectException(ProtocolException::class);

        RocketMqMessageCodec::decodeMessages(substr($record, 0, 60));
    }

    public function test_compressed_messages_are_unsupported(): void
    {
        $record = $this->record('hello', 'orders', '', 0, 0, 0, RocketMqMessageCodec::COMPRESSED_FLAG);

        $this->expectException(UnsupportedFeatureException::class);

        RocketMqMessageCodec::decodeMessages($record);
    }

    private function record(
        string $body,
        string $topic,
        string $properties,
        int $queueOffset = 0,
        int $reconsumeTimes = 0,
        int $physicalOffset = 0,
        int $sysFlag = 0,
    ): string {
        $rest = pack('N', RocketMqMessageCodec::MAGIC_CODE)
            . pack('N', 0)
            . pack('N', 0)
            . pack('N', 0)
            . pack('J', $queueOffset)
            . pack('J', $physicalOffset)
            . pack('N', $sysFlag)
            . pack('J', 1700000000000)
            . (($sysFlag & RocketMqMessageCodec::BORNHOST_V6_FLAG) !== 0 ? str_repeat("\0", 20) : str_repeat("\0", 8))
            . pack('J', 1700000000001)
            . (($sysFlag & RocketMqMessageCodec::STOREHOST_V6_FLAG) !== 0 ? str_repeat("\0", 20) : str_repeat("\0", 8))
            . pack('N', $reconsumeTimes)
            . pack('J', 0)
            . pack('N', strlen($body)) . $body
            . chr(strlen($topic)) . $topic
            . pack('n', strlen($properties)) . $properties;

        return pack('N', 4 + strlen($rest)) . $rest;
    }
}
