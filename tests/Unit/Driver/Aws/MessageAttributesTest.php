<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Aws;

use Erikwang2013\Messaging\Driver\Aws\MessageAttributes;
use PHPUnit\Framework\TestCase;

final class MessageAttributesTest extends TestCase
{
    public function test_scalars_are_string_attributes(): void
    {
        self::assertSame([
            'trace' => ['DataType' => 'String', 'StringValue' => 'abc'],
            'attempt' => ['DataType' => 'String', 'StringValue' => '2'],
            'flag' => ['DataType' => 'String', 'StringValue' => 'true'],
        ], MessageAttributes::encode(['trace' => 'abc', 'attempt' => 2, 'flag' => true]));
    }

    public function test_binary_strings_and_structures_are_binary_attributes(): void
    {
        $encoded = MessageAttributes::encode([
            'blob' => "\x00\xff",
            'meta' => ['a' => 1],
        ]);

        self::assertSame('Binary', $encoded['blob']['DataType']);
        self::assertSame("\x00\xff", base64_decode((string) $encoded['blob']['BinaryValue'], true));
        self::assertSame('{"a":1}', base64_decode((string) $encoded['meta']['BinaryValue'], true));
    }

    public function test_decode_restores_values(): void
    {
        self::assertSame([
            'trace' => 'abc',
            'blob' => "\x00\xff",
            'count' => '7',
        ], MessageAttributes::decode([
            'trace' => ['DataType' => 'String', 'StringValue' => 'abc'],
            'blob' => ['DataType' => 'Binary', 'BinaryValue' => base64_encode("\x00\xff")],
            'count' => ['DataType' => 'Number', 'NumberValue' => '7'],
        ]));
    }

    public function test_invalid_attribute_name_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        MessageAttributes::encode(['aws.reserved' => 'x']);
    }

    public function test_at_most_ten_attributes(): void
    {
        $headers = [];
        for ($i = 0; $i < 11; $i++) {
            $headers['h' . $i] = 'v';
        }

        $this->expectException(\InvalidArgumentException::class);

        MessageAttributes::encode($headers);
    }
}
