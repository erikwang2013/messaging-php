<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

/**
 * Kafka protocol primitive writer (big-endian, length-prefixed).
 */
final class Encoder
{
    private string $buffer = '';

    public function int8(int $value): self
    {
        $this->buffer .= chr($value & 0xff);

        return $this;
    }

    public function int16(int $value): self
    {
        $this->buffer .= pack('n', $value & 0xffff);

        return $this;
    }

    public function int32(int $value): self
    {
        $this->buffer .= pack('N', $value & 0xffffffff);

        return $this;
    }

    public function int64(int $value): self
    {
        $this->buffer .= pack('N2', ($value >> 32) & 0xffffffff, $value & 0xffffffff);

        return $this;
    }

    public function bool(bool $value): self
    {
        return $this->int8($value ? 1 : 0);
    }

    public function string(string $value): self
    {
        return $this->int16(strlen($value))->raw($value);
    }

    public function nullableString(?string $value): self
    {
        if ($value === null) {
            return $this->int16(-1);
        }

        return $this->string($value);
    }

    /**
     * Kafka BYTES: int32 length followed by the payload; null is encoded as -1.
     */
    public function bytes(?string $value): self
    {
        if ($value === null) {
            return $this->int32(-1);
        }

        return $this->int32(strlen($value))->raw($value);
    }

    public function compactString(string $value): self
    {
        return $this->uvarint(strlen($value) + 1)->raw($value);
    }

    public function compactNullableString(?string $value): self
    {
        if ($value === null) {
            return $this->uvarint(0);
        }

        return $this->compactString($value);
    }

    public function uvarint(int $value): self
    {
        if ($value < 0) {
            throw new \InvalidArgumentException('Kafka unsigned varint must not be negative.');
        }

        while (($value & ~0x7f) !== 0) {
            $this->buffer .= chr(($value & 0x7f) | 0x80);
            $value >>= 7;
        }

        $this->buffer .= chr($value);

        return $this;
    }

    /**
     * Kafka signed VARINT uses zigzag encoding.
     */
    public function varint(int $value): self
    {
        return $this->uvarint(($value << 1) ^ ($value >> 63));
    }

    /**
     * Kafka VARLONG uses the same zigzag encoding as VARINT.
     */
    public function varlong(int $value): self
    {
        return $this->varint($value);
    }

    public function raw(string $bytes): self
    {
        $this->buffer .= $bytes;

        return $this;
    }

    public function length(): int
    {
        return strlen($this->buffer);
    }

    public function toString(): string
    {
        return $this->buffer;
    }
}
