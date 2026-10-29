<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Nsq;

final class NsqFrame
{
    public function __construct(
        public readonly int $type,
        public readonly string $payload,
    ) {
    }

    public function isResponse(): bool
    {
        return $this->type === NsqProtocol::FRAME_RESPONSE;
    }

    public function isError(): bool
    {
        return $this->type === NsqProtocol::FRAME_ERROR;
    }

    public function isMessage(): bool
    {
        return $this->type === NsqProtocol::FRAME_MESSAGE;
    }

    public function isHeartbeat(): bool
    {
        return $this->isResponse() && $this->payload === NsqProtocol::HEARTBEAT;
    }

    /**
     * @return array{timestamp: int, attempts: int, id: string, body: string}
     */
    public function message(): array
    {
        return NsqProtocol::parseMessage($this->payload);
    }

    public function errorText(): string
    {
        return rtrim($this->payload, "\r\n");
    }
}
