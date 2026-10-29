<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RocketMq;

final class RocketMqCommand
{
    /**
     * @param array<string, string> $extFields
     */
    public function __construct(
        private int $code,
        private int $opaque,
        private int $flag,
        private ?string $remark,
        private array $extFields,
        private string $body,
    ) {
    }

    public function code(): int
    {
        return $this->code;
    }

    public function opaque(): int
    {
        return $this->opaque;
    }

    public function isResponse(): bool
    {
        return ($this->flag & 0x1) === 0x1;
    }

    public function remark(): ?string
    {
        return $this->remark;
    }

    public function body(): string
    {
        return $this->body;
    }

    /**
     * @return array<string, string>
     */
    public function extFields(): array
    {
        return $this->extFields;
    }

    public function ext(string $name): ?string
    {
        return $this->extFields[$name] ?? null;
    }

    public function extInt(string $name, int $default = 0): int
    {
        $value = $this->extFields[$name] ?? null;

        return $value !== null && preg_match('/\A-?\d+\z/', $value) === 1 ? (int) $value : $default;
    }
}
