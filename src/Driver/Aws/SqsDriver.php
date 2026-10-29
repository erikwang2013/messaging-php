<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Aws;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use GuzzleHttp\ClientInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * AWS SQS driver using the JSON protocol and SigV4.
 */
final class SqsDriver implements DriverInterface
{
    private AwsHttpClient $client;
    private LoggerInterface $logger;
    private float $timeout;
    private int $visibilityTimeout;

    /** @var array<string, string> */
    private array $queueUrls = [];

    public function __construct(ConnectionConfig $config, ?LoggerInterface $logger = null, ?ClientInterface $http = null)
    {
        $awsConfig = AwsConfig::fromConnectionConfig($config);
        $this->logger = $logger ?? new NullLogger();
        $this->client = new AwsHttpClient($awsConfig, $this->logger, $http);
        $this->timeout = $awsConfig->timeout;

        $visibilityTimeout = $config->get('visibility_timeout', 30);
        if (!is_int($visibilityTimeout) || $visibilityTimeout < 0 || $visibilityTimeout > 43200) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [visibility_timeout] must be an integer between 0 and 43200 seconds.',
                $config->name(),
            ));
        }

        $this->visibilityTimeout = $visibilityTimeout;
    }

    public function publish(string $destination, Message $message, array $options = []): PublishResult
    {
        $queueUrl = $this->queueUrl($destination);
        $payload = ['QueueUrl' => $queueUrl, 'MessageBody' => self::body($message)] + $this->messageFields($queueUrl, $message);
        $raw = $this->client->json('sqs', 'AmazonSQS.SendMessage', $queueUrl, $payload);

        $id = $raw['MessageId'] ?? null;

        return new PublishResult($destination, is_string($id) ? $id : null, $raw);
    }

    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        if ($messages === []) {
            return [];
        }

        if (count($messages) > 10) {
            throw new \InvalidArgumentException('SQS SendMessageBatch accepts at most 10 messages.');
        }

        $queueUrl = $this->queueUrl($destination);
        $entries = [];
        foreach ($messages as $index => $message) {
            $entries[] = ['Id' => (string) $index, 'MessageBody' => self::body($message)] + $this->messageFields($queueUrl, $message);
        }

        $raw = $this->client->json('sqs', 'AmazonSQS.SendMessageBatch', $queueUrl, [
            'QueueUrl' => $queueUrl,
            'Entries' => $entries,
        ]);

        $successful = self::entriesById($raw['Successful'] ?? null);
        $failed = self::entriesById($raw['Failed'] ?? null);

        $results = [];
        foreach (array_keys($messages) as $index) {
            $key = (string) $index;
            $entry = $successful[$key] ?? null;
            $id = $entry !== null && isset($entry['MessageId']) && is_string($entry['MessageId']) ? $entry['MessageId'] : null;

            if ($id === null && isset($failed[$key])) {
                $this->logger->warning('SQS batch entry failed', ['entry' => $key, 'failure' => $failed[$key]]);
                $results[] = new PublishResult($destination, null, $failed[$key]);

                continue;
            }

            $results[] = new PublishResult($destination, $id, $entry);
        }

        return $results;
    }

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage
    {
        $queueUrl = $this->queueUrl($destination);
        $deadline = $options->timeoutMs === 0 ? null : microtime(true) + $options->timeoutMs / 1000.0;

        while (true) {
            $remainingMs = $deadline === null ? null : max(0, (int) ceil(($deadline - microtime(true)) * 1000.0));
            $waitSeconds = self::waitSeconds($remainingMs);

            $raw = $this->client->json('sqs', 'AmazonSQS.ReceiveMessage', $queueUrl, [
                'QueueUrl' => $queueUrl,
                'MaxNumberOfMessages' => 1,
                'WaitTimeSeconds' => $waitSeconds,
                'VisibilityTimeout' => $this->visibilityTimeout,
                'AttributeNames' => ['All'],
                'MessageAttributeNames' => ['All'],
            ], max($this->timeout, $waitSeconds + 2.0));

            $messages = $raw['Messages'] ?? null;
            if (is_array($messages) && isset($messages[0]) && is_array($messages[0])) {
                /** @var array<string, mixed> $message */
                $message = $messages[0];

                return $this->receivedMessage($queueUrl, $message);
            }

            if ($deadline === null) {
                continue;
            }

            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                return null;
            }

            usleep((int) (min($remaining, 0.25) * 1_000_000));
            if (microtime(true) >= $deadline) {
                return null;
            }
        }
    }

    public function capabilities(): Capabilities
    {
        return new Capabilities(
            ack: true,
            requeue: true,
            delay: true,
            consumerGroups: false,
            batchPublish: true,
            ordering: false,
            headers: true,
        );
    }

    public function close(): void
    {
        $this->queueUrls = [];
    }

    /**
     * @param array<string, mixed> $message
     */
    private function receivedMessage(string $queueUrl, array $message): ReceivedMessage
    {
        $body = $message['Body'] ?? null;
        $receiptHandle = $message['ReceiptHandle'] ?? null;
        if (!is_string($body) || !is_string($receiptHandle)) {
            throw new ProtocolException('SQS ReceiveMessage response is missing Body or ReceiptHandle.');
        }

        $id = $message['MessageId'] ?? null;
        $key = $message['MessageGroupId'] ?? null;

        $attempts = 1;
        $attributes = $message['Attributes'] ?? null;
        if (is_array($attributes) && isset($attributes['ApproximateReceiveCount']) && is_scalar($attributes['ApproximateReceiveCount'])) {
            $attempts = max(1, (int) $attributes['ApproximateReceiveCount']);
        }

        $rawAttributes = $message['MessageAttributes'] ?? null;
        /** @var array<string, mixed> $rawAttributes */
        $rawAttributes = is_array($rawAttributes) ? $rawAttributes : [];

        return new ReceivedMessage(
            body: $body,
            headers: MessageAttributes::decode($rawAttributes),
            id: is_string($id) ? $id : null,
            key: is_string($key) ? $key : null,
            attempts: $attempts,
            ackCallback: function () use ($queueUrl, $receiptHandle): void {
                $this->deleteMessage($queueUrl, $receiptHandle);
            },
            rejectCallback: function (bool $requeue) use ($queueUrl, $receiptHandle): void {
                if ($requeue) {
                    $this->changeVisibility($queueUrl, $receiptHandle, 0);

                    return;
                }

                $this->deleteMessage($queueUrl, $receiptHandle);
            },
            raw: $message,
        );
    }

    /**
     * @return array<string, mixed>
     */
    private function messageFields(string $queueUrl, Message $message): array
    {
        $fields = [];
        if ($message->headers !== []) {
            $fields['MessageAttributes'] = MessageAttributes::encode($message->headers);
        }

        if ($message->key !== null) {
            if ($message->key === '') {
                throw new \InvalidArgumentException('SQS message group id must not be empty.');
            }

            $fields['MessageGroupId'] = $message->key;
        }

        if ($message->delayMs !== null) {
            if (str_ends_with($queueUrl, '.fifo')) {
                throw new UnsupportedFeatureException('SQS FIFO queues do not support per-message delays.');
            }

            $fields['DelaySeconds'] = self::delaySeconds($message->delayMs);
        }

        return $fields;
    }

    private static function body(Message $message): string
    {
        if ($message->body === '') {
            throw new \InvalidArgumentException('SQS message bodies must not be empty.');
        }

        if (preg_match('//u', $message->body) !== 1) {
            throw new \InvalidArgumentException('SQS message bodies must be valid UTF-8; binary payloads are not supported.');
        }

        return $message->body;
    }

    private static function delaySeconds(int $delayMs): int
    {
        $seconds = (int) ceil($delayMs / 1000);
        if ($seconds > 900) {
            throw new \InvalidArgumentException('SQS message delay must not exceed 900 seconds.');
        }

        return $seconds;
    }

    private static function waitSeconds(?int $remainingMs): int
    {
        if ($remainingMs === null) {
            return 20;
        }

        if ($remainingMs < 1000) {
            return 0;
        }

        return max(1, min(20, intdiv($remainingMs, 1000)));
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private static function entriesById(mixed $entries): array
    {
        $byId = [];
        if (!is_array($entries)) {
            return $byId;
        }

        foreach ($entries as $entry) {
            if (is_array($entry) && isset($entry['Id']) && is_string($entry['Id'])) {
                /** @var array<string, mixed> $entry */
                $byId[$entry['Id']] = $entry;
            }
        }

        return $byId;
    }

    private function queueUrl(string $destination): string
    {
        if (preg_match('~^https?://~i', $destination) === 1) {
            $parts = parse_url($destination);
            if ($parts === false || !isset($parts['host']) || $parts['host'] === '') {
                throw new \InvalidArgumentException(sprintf('Invalid SQS queue URL [%s].', $destination));
            }

            return $destination;
        }

        if (isset($this->queueUrls[$destination])) {
            return $this->queueUrls[$destination];
        }

        if (preg_match('~^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$~', $destination) !== 1) {
            throw new \InvalidArgumentException(sprintf('Invalid SQS queue name [%s].', $destination));
        }

        $raw = $this->client->json('sqs', 'AmazonSQS.GetQueueUrl', $this->client->endpoint('sqs'), [
            'QueueName' => $destination,
        ]);

        $url = $raw['QueueUrl'] ?? null;
        if (!is_string($url) || $url === '') {
            throw new ProtocolException('SQS GetQueueUrl response is missing QueueUrl.');
        }

        return $this->queueUrls[$destination] = $url;
    }

    private function deleteMessage(string $queueUrl, string $receiptHandle): void
    {
        $this->client->json('sqs', 'AmazonSQS.DeleteMessage', $queueUrl, [
            'QueueUrl' => $queueUrl,
            'ReceiptHandle' => $receiptHandle,
        ]);
    }

    private function changeVisibility(string $queueUrl, string $receiptHandle, int $seconds): void
    {
        $this->client->json('sqs', 'AmazonSQS.ChangeMessageVisibility', $queueUrl, [
            'QueueUrl' => $queueUrl,
            'ReceiptHandle' => $receiptHandle,
            'VisibilityTimeout' => $seconds,
        ]);
    }
}
