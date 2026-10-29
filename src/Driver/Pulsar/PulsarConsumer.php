<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Pulsar;

use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Psr\Log\LoggerInterface;

final class PulsarConsumer
{
    private int $permitsOutstanding = 0;

    private bool $closed = false;

    private function __construct(
        private PulsarConnection $connection,
        private string $topic,
        private int $consumerId,
        private ?LoggerInterface $logger = null,
    ) {
    }

    /**
     * @param array<string, mixed> $tlsContext
     */
    public static function subscribe(
        PulsarEndpoint $endpoint,
        string $topic,
        string $subscription,
        ?string $consumerName,
        int $consumerId,
        float $timeout,
        array $tlsContext = [],
        ?LoggerInterface $logger = null,
    ): self {
        $connection = PulsarConnection::openForTopic($endpoint, $topic, $timeout, $tlsContext, $logger);

        try {
            $requestId = $connection->nextRequestId();
            $connection->send(PulsarProtocol::subscribeCommand(
                $topic,
                $subscription,
                $consumerId,
                $requestId,
                $consumerName,
            ));
            $frame = $connection->await(
                PulsarProtocol::COMMAND_SUCCESS,
                microtime(true) + $timeout,
                sprintf('Subscribe to [%s]', $topic),
            );

            if ($frame->intField(1) !== $requestId) {
                throw new ProtocolException(sprintf(
                    'Pulsar subscribe response for [%s] has a mismatched request id.',
                    $topic,
                ));
            }
        } catch (\Throwable $exception) {
            $connection->close();

            throw $exception;
        }

        $logger?->debug('Pulsar consumer subscribed.', [
            'topic' => $topic,
            'subscription' => $subscription,
            'consumer_id' => $consumerId,
        ]);

        return new self($connection, $topic, $consumerId, $logger);
    }

    /**
     * A null deadline blocks until a message arrives or the connection closes.
     */
    public function receive(?float $deadline): ?ReceivedMessage
    {
        if ($this->permitsOutstanding <= 0) {
            $this->connection->send(PulsarProtocol::flowCommand($this->consumerId, 1));
            $this->permitsOutstanding = 1;
        }

        while (true) {
            try {
                $frame = $this->connection->readFrame($deadline);
            } catch (TimeoutException $exception) {
                return null;
            }

            if ($frame->type() === PulsarProtocol::COMMAND_MESSAGE) {
                $this->permitsOutstanding--;

                return $this->decodeMessage($frame);
            }

            if ($frame->type() === PulsarProtocol::COMMAND_ERROR) {
                throw PulsarErrors::exception(
                    $frame->intField(2),
                    $frame->field(3) ?? '',
                    sprintf('Receive from [%s]', $this->topic),
                );
            }

            $this->logger?->debug('Ignoring Pulsar command while receiving.', ['type' => $frame->type()]);
        }
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;

        try {
            $this->connection->send(PulsarProtocol::closeConsumerCommand(
                $this->consumerId,
                $this->connection->nextRequestId(),
            ));
        } catch (\Throwable $exception) {
            $this->logger?->debug('Pulsar consumer close failed.', ['error' => $exception->getMessage()]);
        }

        $this->connection->close();
    }

    private function decodeMessage(PulsarFrame $frame): ReceivedMessage
    {
        if ($frame->intField(1) !== $this->consumerId) {
            throw new ProtocolException('Pulsar message frame targets a different consumer id.');
        }

        $encodedId = $frame->field(2);
        if ($encodedId === null) {
            throw new ProtocolException('Pulsar message frame is missing its message id.');
        }

        $messageId = PulsarProtocol::decodeMessageId($encodedId);
        if ($messageId['batch_index'] >= 0) {
            throw new UnsupportedFeatureException('Pulsar batched messages are not supported.');
        }

        $payloadFrame = $frame->payload();
        if ($payloadFrame === null) {
            throw new ProtocolException('Pulsar message frame is missing its payload.');
        }

        [$metadata, $body] = PulsarProtocol::decodePayload($payloadFrame);
        $redeliveryCount = $frame->intField(3, 0);
        $settled = false;
        $connection = $this->connection;
        $consumerId = $this->consumerId;

        return new ReceivedMessage(
            body: $body,
            headers: PulsarProtocol::decodeProperties($metadata),
            id: PulsarProtocol::messageIdString($messageId),
            key: PulsarProtocol::decodeKey($metadata),
            attempts: $redeliveryCount + 1,
            ackCallback: function () use (&$settled, $connection, $consumerId, $encodedId): void {
                if ($settled) {
                    return;
                }

                $settled = true;
                $connection->send(PulsarProtocol::ackCommand($consumerId, $encodedId));
            },
            rejectCallback: function (bool $requeue) use (&$settled, $connection, $consumerId, $encodedId): void {
                if ($settled) {
                    return;
                }

                $settled = true;

                if ($requeue) {
                    $connection->send(PulsarProtocol::redeliverCommand($consumerId, $encodedId));

                    return;
                }

                $connection->send(PulsarProtocol::ackCommand($consumerId, $encodedId));
            },
            raw: [
                'topic' => $this->topic,
                'ledger_id' => $messageId['ledger_id'],
                'entry_id' => $messageId['entry_id'],
                'partition' => $messageId['partition'],
                'redelivery_count' => $redeliveryCount,
            ],
        );
    }
}
