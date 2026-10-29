<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Redis;

use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Redis\Resp\RespClient;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;

final class RedisStreamsDriver implements DriverInterface
{
    /** 24h cap stands in for an infinite socket read; stream_set_timeout(0) does not block forever. */
    private const INDEFINITE_READ_TIMEOUT = 86_400.0;

    /** @var array<string, true> */
    private array $groups = [];

    public function __construct(
        private RespClient $client,
        private string $group,
        private string $consumer,
        private ?int $maxLength = null,
    ) {
    }

    public function publish(string $destination, Message $message, array $options = []): PublishResult
    {
        $this->assertSupported($message);

        $id = $this->client->command(...StreamsCommand::add(
            $destination,
            $message->body,
            $message->headers,
            $message->key,
            $this->maxLength,
        ));

        if (!is_string($id)) {
            throw new ProtocolException('Redis XADD did not return a stream id.');
        }

        return new PublishResult($destination, $id, $id);
    }

    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        foreach ($messages as $message) {
            $this->assertSupported($message);
        }

        $results = [];
        foreach ($messages as $message) {
            $results[] = $this->publish($destination, $message, $options);
        }

        return $results;
    }

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage
    {
        $this->ensureGroup($destination);
        $this->client->setReadTimeout(
            $options->timeoutMs === 0
                ? self::INDEFINITE_READ_TIMEOUT
                : $options->timeoutMs / 1000 + 5.0,
        );

        try {
            $reply = $this->readGroup($destination, $options->timeoutMs);
        } finally {
            $this->client->resetReadTimeout();
        }

        if ($reply === null) {
            return null;
        }

        [$id, $fields] = $this->parseEntry($reply);
        $parsed = $this->parseFields($fields);

        return new ReceivedMessage(
            $parsed['body'],
            $parsed['headers'],
            $id,
            $parsed['key'],
            1,
            function () use ($destination, $id): void {
                $this->acknowledge($destination, $id);
            },
            function (bool $requeue) use ($destination, $id): void {
                if ($requeue) {
                    throw new UnsupportedFeatureException(
                        'Redis Streams cannot synchronously requeue a message; leave it unacknowledged '
                        . 'and recover it with XAUTOCLAIM from another consumer.',
                    );
                }

                $this->acknowledge($destination, $id);
            },
            $fields,
        );
    }

    public function capabilities(): Capabilities
    {
        return new Capabilities(
            ack: true,
            requeue: false,
            delay: false,
            consumerGroups: true,
            batchPublish: true,
            ordering: true,
            headers: true,
        );
    }

    public function close(): void
    {
        $this->groups = [];
        $this->client->close();
    }

    private function readGroup(string $destination, int $timeoutMs): mixed
    {
        try {
            return $this->client->command(...StreamsCommand::readGroup(
                $destination,
                $this->group,
                $this->consumer,
                $timeoutMs,
            ));
        } catch (ProtocolException $exception) {
            if (!str_contains($exception->getMessage(), 'NOGROUP')) {
                throw $exception;
            }

            // The stream (and with it the group) was deleted; recreate and retry once.
            unset($this->groups[$destination]);
            $this->ensureGroup($destination);

            return $this->client->command(...StreamsCommand::readGroup(
                $destination,
                $this->group,
                $this->consumer,
                $timeoutMs,
            ));
        }
    }

    private function ensureGroup(string $destination): void
    {
        if (isset($this->groups[$destination])) {
            return;
        }

        try {
            $this->client->command(...StreamsCommand::createGroup($destination, $this->group));
        } catch (ProtocolException $exception) {
            if (!str_starts_with($exception->getMessage(), 'BUSYGROUP')) {
                throw $exception;
            }
        }

        $this->groups[$destination] = true;
    }

    private function acknowledge(string $destination, string $id): void
    {
        $this->client->command(...StreamsCommand::ack($destination, $this->group, $id));
    }

    private function assertSupported(Message $message): void
    {
        if ($message->delayMs !== null) {
            throw new UnsupportedFeatureException('Redis Streams does not support delayed messages.');
        }
    }

    /**
     * @return array{string, list<mixed>}
     */
    private function parseEntry(mixed $reply): array
    {
        $stream = is_array($reply) ? ($reply[0] ?? null) : null;
        $entries = is_array($stream) ? ($stream[1] ?? null) : null;
        $entry = is_array($entries) ? ($entries[0] ?? null) : null;

        if (!is_array($entry) || !is_string($entry[0] ?? null) || !is_array($entry[1] ?? null)) {
            throw new ProtocolException('Malformed XREADGROUP reply.');
        }

        return [$entry[0], array_values($entry[1])];
    }

    /**
     * @param list<mixed> $fields
     * @return array{body: string, headers: array<string, mixed>, key: ?string}
     */
    private function parseFields(array $fields): array
    {
        $values = [];
        $count = count($fields);
        if ($count % 2 !== 0) {
            throw new ProtocolException('Malformed Redis Streams entry: odd field count.');
        }

        for ($i = 0; $i < $count; $i += 2) {
            $name = $fields[$i];
            if (!is_string($name)) {
                throw new ProtocolException('Malformed Redis Streams entry field.');
            }

            $values[$name] = $fields[$i + 1];
        }

        $body = $values['body'] ?? null;
        if (!is_string($body)) {
            throw new ProtocolException('Redis Streams entry is missing its [body] field.');
        }

        $headers = [];
        $encodedHeaders = $values['headers'] ?? null;
        if ($encodedHeaders !== null) {
            if (!is_string($encodedHeaders)) {
                throw new ProtocolException('Malformed Redis Streams [headers] field.');
            }

            try {
                $decoded = json_decode($encodedHeaders, true, 512, JSON_THROW_ON_ERROR);
            } catch (\JsonException $exception) {
                throw new ProtocolException('Malformed JSON in Redis Streams [headers] field.', 0, $exception);
            }

            if (!is_array($decoded)) {
                throw new ProtocolException('Redis Streams [headers] field must decode to an object.');
            }

            $headers = $decoded;
        }

        $key = $values['key'] ?? null;
        if ($key !== null && !is_string($key)) {
            throw new ProtocolException('Malformed Redis Streams [key] field.');
        }

        return ['body' => $body, 'headers' => $headers, 'key' => $key];
    }
}
