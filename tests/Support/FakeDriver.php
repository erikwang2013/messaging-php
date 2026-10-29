<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Support;

use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Contract\ReceiveOptions;

final class FakeDriver implements DriverInterface
{
    /** @var array<string, list<array{id: string, message: Message}>> */
    private array $queues = [];

    /** @var array<string, array{id: string, message: Message}> */
    private array $inFlight = [];

    private int $sequence = 0;

    public function __construct(private readonly ?Capabilities $capabilities = null)
    {
    }

    public function publish(string $destination, Message $message, array $options = []): PublishResult
    {
        $id = 'fake-' . ++$this->sequence;
        $this->queues[$destination][] = ['id' => $id, 'message' => $message];

        return new PublishResult($destination, $id);
    }

    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        return array_map(
            fn (Message $message): PublishResult => $this->publish($destination, $message, $options),
            $messages,
        );
    }

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage
    {
        $queue = $this->queues[$destination] ?? [];
        $entry = array_shift($queue);
        $this->queues[$destination] = $queue;

        if ($entry === null) {
            return null;
        }

        $this->inFlight[$entry['id']] = $entry;

        return new ReceivedMessage(
            body: $entry['message']->body,
            headers: $entry['message']->headers,
            id: $entry['id'],
            key: $entry['message']->key,
            ackCallback: function () use ($entry): void {
                unset($this->inFlight[$entry['id']]);
            },
            rejectCallback: function (bool $requeue) use ($entry, $destination): void {
                unset($this->inFlight[$entry['id']]);

                if ($requeue) {
                    array_unshift($this->queues[$destination], $entry);
                }
            },
        );
    }

    public function capabilities(): Capabilities
    {
        return $this->capabilities ?? new Capabilities(ack: true, requeue: true, batchPublish: true);
    }

    public function close(): void
    {
    }
}
