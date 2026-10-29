<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Pulsar;

use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Psr\Log\LoggerInterface;

final class PulsarDriver implements DriverInterface
{
    /** @var array<string, PulsarProducer> */
    private array $producers = [];

    /** @var array<string, PulsarConsumer> */
    private array $consumers = [];

    private int $nextId = 0;

    /**
     * @param array<string, mixed> $tlsContext
     */
    public function __construct(
        private PulsarEndpoint $endpoint,
        private ?string $defaultTopic,
        private string $subscription,
        private ?string $consumerName,
        private float $timeout,
        private array $tlsContext = [],
        private ?LoggerInterface $logger = null,
    ) {
    }

    public function publish(string $destination, Message $message, array $options = []): PublishResult
    {
        $topic = $this->resolveTopic($destination);
        $producer = $this->producers[$topic] ??= PulsarProducer::create(
            $this->endpoint,
            $topic,
            ++$this->nextId,
            $this->timeout,
            $this->tlsContext,
            $this->logger,
        );

        try {
            return $producer->publish($message);
        } catch (ConnectionException $exception) {
            $this->forgetProducer($topic);

            throw $exception;
        }
    }

    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        throw new UnsupportedFeatureException('Pulsar batching is not supported.');
    }

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage
    {
        $topic = $this->resolveTopic($destination);
        $consumer = $this->consumers[$topic] ??= PulsarConsumer::subscribe(
            $this->endpoint,
            $topic,
            $this->subscription,
            $this->consumerName,
            ++$this->nextId,
            $this->timeout,
            $this->tlsContext,
            $this->logger,
        );

        $deadline = $options->timeoutMs === 0
            ? null
            : microtime(true) + $options->timeoutMs / 1000;

        try {
            return $consumer->receive($deadline);
        } catch (ConnectionException | ProtocolException $exception) {
            $this->forgetConsumer($topic);

            throw $exception;
        }
    }

    public function capabilities(): Capabilities
    {
        return new Capabilities(
            ack: true,
            requeue: true,
            delay: true,
            consumerGroups: true,
            batchPublish: false,
            ordering: true,
            headers: true,
            requiresActiveSubscription: false,
        );
    }

    public function close(): void
    {
        foreach ($this->producers as $producer) {
            $producer->close();
        }

        foreach ($this->consumers as $consumer) {
            $consumer->close();
        }

        $this->producers = [];
        $this->consumers = [];
    }

    private function resolveTopic(string $destination): string
    {
        if ($destination !== '') {
            return $destination;
        }

        if ($this->defaultTopic !== null) {
            return $this->defaultTopic;
        }

        throw new \InvalidArgumentException(
            'A Pulsar topic is required: pass a destination or configure the [topic] option.',
        );
    }

    private function forgetProducer(string $topic): void
    {
        if (!isset($this->producers[$topic])) {
            return;
        }

        $this->producers[$topic]->close();
        unset($this->producers[$topic]);
    }

    private function forgetConsumer(string $topic): void
    {
        if (!isset($this->consumers[$topic])) {
            return;
        }

        $this->consumers[$topic]->close();
        unset($this->consumers[$topic]);
    }
}
