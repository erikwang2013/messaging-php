<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Contract;

interface DriverInterface
{
    /**
     * @param array<string, mixed> $options
     */
    public function publish(string $destination, Message $message, array $options = []): PublishResult;

    /**
     * @param list<Message> $messages
     * @param array<string, mixed> $options
     * @return list<PublishResult>
     */
    public function publishBatch(string $destination, array $messages, array $options = []): array;

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage;

    public function capabilities(): Capabilities;

    public function close(): void;
}
