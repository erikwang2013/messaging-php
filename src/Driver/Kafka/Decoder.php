<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

use Erikwang2013\Messaging\Exception\ProtocolException;

/**
 * Bounds-checked Kafka protocol reader.
 */
final class Decoder
{
    private int $offset = 0;

    public function __construct(private string $buffer)
    {
    }

    public function remaining(): int
    {
        return strlen($this->buffer) - $this->offset;
    }

    public function position(): int
    {
        return $this->offset;
    }

    public function int8(): int
    {
        $value = ord($this->take(1));

        return $value >= 0x80 ? $value - 0x100 : $value;
    }

    public function int16(): int
    {
        $value = unpack('n', $this->take(2));
        if ($value === false) {
            throw new ProtocolException('Unable to decode a Kafka INT16.');
        }

        $value = (int) $value[1];

        return $value >= 0x8000 ? $value - 0x10000 : $value;
    }

    public function int32(): int
    {
        $value = unpack('N', $this->take(4));
        if ($value === false) {
            throw new ProtocolException('Unable to decode a Kafka INT32.');
        }

        $value = (int) $value[1];

        return $value >= 0x80000000 ? $value - 0x100000000 : $value;
    }

    public function int64(): int
    {
        $parts = unpack('Nhi/Nlo', $this->take(8));
        if ($parts === false) {
            throw new ProtocolException('Unable to decode a Kafka INT64.');
        }

        $hi = (int) $parts['hi'];
        $lo = (int) $parts['lo'];

        if (($hi & 0x80000000) !== 0) {
            $hi = ~$hi & 0xffffffff;
            $lo = ~$lo & 0xffffffff;

            return -((($hi << 32) | $lo) + 1);
        }

        return ($hi << 32) | $lo;
    }

    public function bool(): bool
    {
        return $this->int8() !== 0;
    }

    public function string(): string
    {
        $length = $this->int16();
        if ($length < 0) {
            throw new ProtocolException('Kafka response contained a null non-nullable STRING.');
        }

        return $this->take($length);
    }

    public function nullableString(): ?string
    {
        $length = $this->int16();
        if ($length === -1) {
            return null;
        }

        if ($length < 0) {
            throw new ProtocolException(sprintf('Invalid Kafka STRING length [%d].', $length));
        }

        return $this->take($length);
    }

    public function bytes(): ?string
    {
        $length = $this->int32();
        if ($length === -1) {
            return null;
        }

        if ($length < 0) {
            throw new ProtocolException(sprintf('Invalid Kafka BYTES length [%d].', $length));
        }

        return $this->take($length);
    }

    public function compactString(): string
    {
        $length = $this->uvarint();
        if ($length === 0) {
            throw new ProtocolException('Kafka response contained a null non-nullable COMPACT_STRING.');
        }

        return $this->take($length - 1);
    }

    public function compactNullableString(): ?string
    {
        $length = $this->uvarint();
        if ($length === 0) {
            return null;
        }

        return $this->take($length - 1);
    }

    public function uvarint(): int
    {
        $result = 0;
        $shift = 0;

        while (true) {
            $byte = ord($this->take(1));
            $result |= ($byte & 0x7f) << $shift;

            if (($byte & 0x80) === 0) {
                return $result;
            }

            $shift += 7;
            if ($shift > 63) {
                throw new ProtocolException('Kafka varint is too long.');
            }
        }
    }

    public function varint(): int
    {
        $value = $this->uvarint();

        return ($value >> 1) ^ -($value & 1);
    }

    public function varlong(): int
    {
        return $this->varint();
    }

    /**
     * Skips a flexible-version tagged-field buffer.
     */
    public function skipTags(): void
    {
        $count = $this->uvarint();
        for ($i = 0; $i < $count; $i++) {
            $this->uvarint();
            $this->take($this->uvarint());
        }
    }

    /**
     * Returns the array element count, or null for a null array (length -1).
     */
    public function arrayLength(): ?int
    {
        $length = $this->int32();

        return $length === -1 ? null : $length;
    }

    public function take(int $length): string
    {
        if ($length < 0 || $length > $this->remaining()) {
            throw new ProtocolException(sprintf(
                'Truncated Kafka response: needed %d bytes, %d remaining.',
                $length,
                $this->remaining(),
            ));
        }

        $chunk = substr($this->buffer, $this->offset, $length);
        $this->offset += $length;

        return $chunk;
    }

    public function skip(int $length): void
    {
        $this->take($length);
    }

    public function rest(): string
    {
        return $this->take($this->remaining());
    }
}
