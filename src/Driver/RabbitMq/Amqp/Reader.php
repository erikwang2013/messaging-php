<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RabbitMq\Amqp;

use Erikwang2013\Messaging\Exception\ProtocolException;

/**
 * Sequential reader over an AMQP method payload or content header.
 */
final class Reader
{
    private int $offset = 0;

    public function __construct(private string $buffer)
    {
    }

    public function position(): int
    {
        return $this->offset;
    }

    public function remaining(): int
    {
        return strlen($this->buffer) - $this->offset;
    }

    public function read(int $length): string
    {
        if ($length < 0 || $length > $this->remaining()) {
            throw new ProtocolException(sprintf(
                'Malformed AMQP payload: needed %d more bytes, %d available.',
                $length,
                $this->remaining(),
            ));
        }

        $slice = substr($this->buffer, $this->offset, $length);
        $this->offset += $length;

        return $slice;
    }

    public function octet(): int
    {
        return ord($this->read(1));
    }

    public function short(): int
    {
        return (int) unpack('n', $this->read(2))[1];
    }

    public function long(): int
    {
        return (int) unpack('N', $this->read(4))[1];
    }

    public function longlong(): int
    {
        return (int) unpack('J', $this->read(8))[1];
    }

    public function shortstr(): string
    {
        return $this->read($this->octet());
    }

    public function longstr(): string
    {
        return $this->read($this->long());
    }

    /**
     * Reads $count consecutive bits packed into octets, LSB first.
     *
     * @return list<bool>
     */
    public function bits(int $count): array
    {
        $bits = [];
        $octet = 0;
        $position = 0;

        for ($i = 0; $i < $count; $i++) {
            if ($position === 0) {
                $octet = $this->octet();
            }

            $bits[] = ($octet & (1 << $position)) !== 0;
            $position = ($position + 1) % 8;
        }

        return $bits;
    }

    /**
     * @return array<string, mixed>
     */
    public function table(): array
    {
        return FieldTable::decode($this);
    }
}
