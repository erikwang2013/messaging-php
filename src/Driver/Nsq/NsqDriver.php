<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Nsq;

use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Psr\Log\LoggerInterface;

final class NsqDriver implements DriverInterface
{
    private ?NsqConnection $publisher = null;

    /** @var array<string, NsqConnection> */
    private array $consumers = [];

    public function __construct(
        private string $host,
        private int $port,
        private string $channel,
        private float $timeout = 5.0,
        private int $requeueDelayMs = 0,
        private ?LoggerInterface $logger = null,
    ) {
    }

    public function publish(string $destination, Message $message, array $options = []): PublishResult
    {
        if ($message->headers !== []) {
            throw new UnsupportedFeatureException('NSQ does not support message headers.');
        }

        if ($message->key !== null) {
            throw new UnsupportedFeatureException('NSQ does not support message keys.');
        }

        if ($message->delayMs !== null) {
            throw new UnsupportedFeatureException('NSQ does not support publish delays.');
        }

        $this->publisher ??= $this->connect();

        try {
            $this->publisher->publish($destination, $message->body);
        } catch (ConnectionException $exception) {
            $this->publisher->close();
            $this->publisher = null;

            throw $exception;
        }

        // NSQ only assigns a message id when the message is delivered.
        return new PublishResult($destination);
    }

    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        throw new UnsupportedFeatureException('NSQ does not support batch publishing.');
    }

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage
    {
        $connection = $this->consumers[$destination] ??= $this->openConsumer($destination);
        $deadline = $options->timeoutMs === 0 ? null : microtime(true) + $options->timeoutMs / 1000;

        try {
            $message = $connection->receiveMessage($deadline);
        } catch (TimeoutException) {
            return null;
        } catch (ProtocolException | ConnectionException $exception) {
            $this->forgetConsumer($destination);

            throw $exception;
        }

        $settled = false;

        return new ReceivedMessage(
            body: $message['body'],
            id: $message['id'],
            attempts: $message['attempts'],
            ackCallback: function () use ($connection, $message, &$settled): void {
                if ($settled) {
                    return;
                }

                $settled = true;
                $connection->finish($message['id']);
            },
            rejectCallback: function (bool $requeue) use ($connection, $message, &$settled): void {
                if ($settled) {
                    return;
                }

                $settled = true;

                if ($requeue) {
                    $connection->requeue($message['id'], $this->requeueDelayMs);

                    return;
                }

                $connection->finish($message['id']);
            },
            raw: $message,
        );
    }

    public function capabilities(): Capabilities
    {
        return new Capabilities(
            ack: true,
            requeue: true,
            delay: false,
            consumerGroups: false,
            batchPublish: false,
            ordering: false,
            headers: false,
        );
    }

    public function close(): void
    {
        $this->publisher?->close();
        $this->publisher = null;

        foreach ($this->consumers as $connection) {
            $connection->close();
        }

        $this->consumers = [];
    }

    private function openConsumer(string $destination): NsqConnection
    {
        $connection = $this->connect();

        try {
            $connection->subscribe($destination, $this->channel);
        } catch (\Throwable $exception) {
            $connection->close();

            throw $exception;
        }

        return $connection;
    }

    private function connect(): NsqConnection
    {
        $this->logger?->debug('Connecting to NSQ.', ['host' => $this->host, 'port' => $this->port]);

        return NsqConnection::connect($this->host, $this->port, $this->timeout);
    }

    private function forgetConsumer(string $destination): void
    {
        if (!isset($this->consumers[$destination])) {
            return;
        }

        $this->consumers[$destination]->close();
        unset($this->consumers[$destination]);
    }
}
