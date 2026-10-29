<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Pulsar;

final class PulsarFrame
{
    /**
     * @param array<int, list<int|string>> $fields decoded BaseCommand
     */
    public function __construct(
        private int $type,
        private array $fields,
        private ?string $payload,
    ) {
    }

    public function type(): int
    {
        return $this->type;
    }

    public function payload(): ?string
    {
        return $this->payload;
    }

    /**
     * Returns the first length-delimited value for a field.
     */
    public function field(int $number): ?string
    {
        return Protobuf::stringValue($this->fields, $number);
    }

    public function intField(int $number, int $default = 0): int
    {
        return Protobuf::intValue($this->fields, $number, $default);
    }
}
