<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Pulsar;

use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Psr\Log\LoggerInterface;

final class PulsarProducer
{
    private int $sequenceId = 0;

    private bool $closed = false;

    private function __construct(
        private PulsarConnection $connection,
        private string $topic,
        private int $producerId,
        private string $producerName,
        private float $timeout,
        private ?LoggerInterface $logger = null,
    ) {
    }

    /**
     * @param array<string, mixed> $tlsContext
     */
    public static function create(
        PulsarEndpoint $endpoint,
        string $topic,
        int $producerId,
        float $timeout,
        array $tlsContext = [],
        ?LoggerInterface $logger = null,
    ): self {
        $connection = PulsarConnection::openForTopic($endpoint, $topic, $timeout, $tlsContext, $logger);
        $producerName = 'messaging-php-' . bin2hex(random_bytes(8));

        try {
            $requestId = $connection->nextRequestId();
            $connection->send(PulsarProtocol::producerCommand($topic, $producerId, $requestId, $producerName));
            $frame = $connection->await(
                PulsarProtocol::COMMAND_PRODUCER_SUCCESS,
                microtime(true) + $timeout,
                sprintf('Create producer for [%s]', $topic),
            );

            if ($frame->intField(1) !== $requestId) {
                throw new ProtocolException(sprintf(
                    'Pulsar producer response for [%s] has a mismatched request id.',
                    $topic,
                ));
            }

            $assignedName = $frame->field(2);
            if ($assignedName !== null && $assignedName !== '') {
                $producerName = $assignedName;
            }
        } catch (\Throwable $exception) {
            $connection->close();

            throw $exception;
        }

        $logger?->debug('Pulsar producer created.', ['topic' => $topic, 'producer_id' => $producerId]);

        return new self($connection, $topic, $producerId, $producerName, $timeout, $logger);
    }

    public function publish(Message $message): PublishResult
    {
        $sequenceId = $this->sequenceId;
        $this->sequenceId++;

        $deliverAt = $message->delayMs !== null ? PulsarProtocol::nowMs() + $message->delayMs : null;
        $payload = PulsarProtocol::encodePayload(
            $this->producerName,
            $sequenceId,
            $message->body,
            $message->key,
            self::normalizeHeaders($message->headers),
            $deliverAt,
        );

        $this->connection->send(PulsarProtocol::sendCommand($this->producerId, $sequenceId), $payload);

        $deadline = microtime(true) + $this->timeout;

        while (true) {
            $frame = $this->connection->readFrame($deadline);
            $type = $frame->type();

            if ($type === PulsarProtocol::COMMAND_SEND_RECEIPT) {
                if ($frame->intField(1) !== $this->producerId || $frame->intField(2) !== $sequenceId) {
                    throw new ProtocolException('Pulsar send receipt does not match the pending send.');
                }

                $id = null;
                $raw = [
                    'topic' => $this->topic,
                    'producer_id' => $this->producerId,
                    'sequence_id' => $sequenceId,
                ];

                $encodedId = $frame->field(3);
                if ($encodedId !== null) {
                    $messageId = PulsarProtocol::decodeMessageId($encodedId);
                    $id = PulsarProtocol::messageIdString($messageId);
                    $raw += $messageId;
                }

                return new PublishResult($this->topic, $id, $raw);
            }

            if ($type === PulsarProtocol::COMMAND_SEND_ERROR) {
                throw PulsarErrors::exception(
                    $frame->intField(3),
                    $frame->field(4) ?? '',
                    sprintf('Send to [%s]', $this->topic),
                );
            }

            if ($type === PulsarProtocol::COMMAND_ERROR) {
                throw PulsarErrors::exception(
                    $frame->intField(2),
                    $frame->field(3) ?? '',
                    sprintf('Send to [%s]', $this->topic),
                );
            }

            throw new ProtocolException(sprintf(
                'Unexpected Pulsar command [%d] while waiting for a send receipt.',
                $type,
            ));
        }
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;

        try {
            $this->connection->send(PulsarProtocol::closeProducerCommand(
                $this->producerId,
                $this->connection->nextRequestId(),
            ));
        } catch (\Throwable $exception) {
            $this->logger?->debug('Pulsar producer close failed.', ['error' => $exception->getMessage()]);
        }

        $this->connection->close();
    }

    /**
     * @param array<string, mixed> $headers
     * @return array<string, string>
     */
    private static function normalizeHeaders(array $headers): array
    {
        $normalized = [];

        foreach ($headers as $name => $value) {
            $name = (string) $name;

            if ($value === null) {
                $normalized[$name] = '';
                continue;
            }

            if (is_bool($value)) {
                $normalized[$name] = $value ? 'true' : 'false';
                continue;
            }

            if (is_scalar($value)) {
                $normalized[$name] = (string) $value;
                continue;
            }

            if (is_array($value) || is_object($value)) {
                $normalized[$name] = (string) json_encode($value, JSON_THROW_ON_ERROR);
                continue;
            }

            throw new UnsupportedFeatureException(sprintf(
                'Pulsar header [%s] has an unsupported value type [%s].',
                $name,
                get_debug_type($value),
            ));
        }

        return $normalized;
    }
}
