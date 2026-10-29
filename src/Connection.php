<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging;

use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\ConsumeOptions;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;

final class Connection
{
    public function __construct(
        private string $name,
        private DriverInterface $driver,
    ) {
    }

    public function name(): string
    {
        return $this->name;
    }

    /**
     * @param array<string, mixed> $options
     */
    public function publish(string $destination, Message|string $message, array $options = []): PublishResult
    {
        return $this->driver->publish(
            $destination,
            is_string($message) ? new Message($message) : $message,
            $options,
        );
    }

    /**
     * @param list<Message|string> $messages
     * @param array<string, mixed> $options
     * @return list<PublishResult>
     */
    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        if (!$this->driver->capabilities()->batchPublish) {
            throw new UnsupportedFeatureException(sprintf(
                'Driver [%s] does not support batch publishing.',
                $this->name,
            ));
        }

        $normalized = array_map(
            static fn (Message|string $message): Message => is_string($message) ? new Message($message) : $message,
            $messages,
        );

        return $this->driver->publishBatch($destination, $normalized, $options);
    }

    public function receive(string $destination, ?ReceiveOptions $options = null): ?ReceivedMessage
    {
        return $this->driver->receive($destination, $options ?? new ReceiveOptions());
    }

    public function consume(string $destination, callable $handler, ?ConsumeOptions $options = null): void
    {
        $options ??= new ConsumeOptions();
        $processed = 0;

        while ($options->maxMessages === null || $processed < $options->maxMessages) {
            $message = $this->receive($destination, new ReceiveOptions($options->timeoutMs));
            if ($message === null) {
                continue;
            }

            $handler($message);
            $processed++;
        }
    }

    public function capabilities(): Capabilities
    {
        return $this->driver->capabilities();
    }

    public function close(): void
    {
        $this->driver->close();
    }
}
