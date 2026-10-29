<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Gcp;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\RequestException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use GuzzleHttp\Client;
use GuzzleHttp\ClientInterface;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\GuzzleException;
use Psr\Log\LoggerInterface;

/**
 * Pure-PHP Google Cloud Pub/Sub REST driver (v1 JSON API).
 *
 * Publish targets a topic, receive targets a subscription; both must exist.
 */
final class GcpPubSubDriver implements DriverInterface
{
    private const DEFAULT_ENDPOINT = 'https://pubsub.googleapis.com';

    /** Pub/Sub caps a single REST pull at roughly 90 seconds. */
    private const LONG_POLL_TIMEOUT = 95.0;

    private string $project;
    private string $endpoint;
    private float $timeout;
    private int $ackDeadlineSeconds;
    private ?string $defaultSubscription;
    private ClientInterface $http;
    private ?ServiceAccountTokenProvider $auth;
    private ?LoggerInterface $logger;

    public function __construct(
        ConnectionConfig $config,
        ?LoggerInterface $logger = null,
        ?ClientInterface $http = null,
    ) {
        $this->project = $config->requireString('project_id');

        $emulator = $config->get('emulator_host');
        if ($emulator !== null && (!is_string($emulator) || $emulator === '')) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [emulator_host] must be a non-empty string or null.',
                $config->name(),
            ));
        }

        $endpoint = $config->get('endpoint');
        if ($endpoint !== null && (!is_string($endpoint) || $endpoint === '')) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [endpoint] must be a non-empty string or null.',
                $config->name(),
            ));
        }
        $this->endpoint = rtrim($endpoint ?? ($emulator !== null ? self::emulatorEndpoint($emulator) : self::DEFAULT_ENDPOINT), '/');

        $timeout = $config->get('timeout', 30.0);
        if ((!is_int($timeout) && !is_float($timeout)) || $timeout <= 0) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [timeout] must be a positive number.',
                $config->name(),
            ));
        }
        $this->timeout = (float) $timeout;

        $subscription = $config->get('subscription');
        if ($subscription !== null && (!is_string($subscription) || $subscription === '')) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [subscription] must be a non-empty string or null.',
                $config->name(),
            ));
        }
        $this->defaultSubscription = $subscription;

        $ackDeadline = $config->get('ack_deadline_seconds', 60);
        if (!is_int($ackDeadline) || $ackDeadline < 0 || $ackDeadline > 600) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [ack_deadline_seconds] must be an integer between 0 and 600.',
                $config->name(),
            ));
        }
        $this->ackDeadlineSeconds = $ackDeadline;

        $this->logger = $logger;
        $this->http = $http ?? new Client(['http_errors' => false]);
        $this->auth = $emulator === null
            ? new ServiceAccountTokenProvider(self::loadCredentials($config), $this->http, $this->timeout, $logger)
            : null;
    }

    public function publish(string $destination, Message $message, array $options = []): PublishResult
    {
        $results = $this->publishBatch($destination, [$message], $options);

        return $results[0];
    }

    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        if ($messages === []) {
            throw new \InvalidArgumentException('Pub/Sub batch publishing requires at least one message.');
        }

        $encoded = [];
        foreach ($messages as $message) {
            if ($message->delayMs !== null) {
                throw new UnsupportedFeatureException('Google Cloud Pub/Sub does not support delayed publishing.');
            }

            $entry = ['data' => base64_encode($message->body)];
            if ($message->headers !== []) {
                $entry['attributes'] = self::attributes($message->headers);
            }
            if ($message->key !== null) {
                $entry['orderingKey'] = $message->key;
            }
            $encoded[] = $entry;
        }

        $data = $this->send('POST', $this->topicPath($destination) . ':publish', ['messages' => $encoded], $this->timeout);

        $ids = $data['messageIds'] ?? null;
        if (!is_array($ids) || count($ids) !== count($encoded)) {
            throw new ProtocolException('Pub/Sub publish response did not contain one message id per message.');
        }
        $ids = array_values($ids);

        $results = [];
        foreach (array_keys($encoded) as $index) {
            $id = $ids[$index] ?? null;
            if (!is_string($id) || $id === '') {
                throw new ProtocolException('Pub/Sub publish response contained an invalid message id.');
            }
            $results[] = new PublishResult($destination, $id, $id);
        }

        return $results;
    }

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage
    {
        $subscription = $destination === '' ? $this->defaultSubscription : $destination;
        if ($subscription === null || $subscription === '') {
            throw new \InvalidArgumentException('Pub/Sub receive requires a subscription name or the [subscription] option.');
        }

        $path = $this->subscriptionPath($subscription) . ':pull';
        $body = ['maxMessages' => 1, 'returnImmediately' => false];

        if ($options->timeoutMs === 0) {
            while (true) {
                $message = $this->firstReceived($this->send('POST', $path, $body, self::LONG_POLL_TIMEOUT), $subscription);
                if ($message !== null) {
                    return $message;
                }
                // Servers without long-poll support answer empty immediately; keep the loop cool.
                usleep(10_000);
            }
        }

        $deadline = microtime(true) + $options->timeoutMs / 1000;
        while (($remaining = $deadline - microtime(true)) > 0) {
            try {
                // Clamp to 1ms: a sub-millisecond timeout rounds to 0 in cURL, which means "no timeout".
                $data = $this->send('POST', $path, $body, max($remaining, 0.001));
            } catch (TimeoutException) {
                return null;
            }

            $message = $this->firstReceived($data, $subscription);
            if ($message !== null) {
                return $message;
            }

            usleep(10_000);
        }

        return null;
    }

    public function capabilities(): Capabilities
    {
        return new Capabilities(
            ack: true,
            requeue: true,
            delay: false,
            consumerGroups: true,
            batchPublish: true,
            ordering: true,
            headers: true,
            requiresActiveSubscription: false,
        );
    }

    public function close(): void
    {
        // Guzzle keeps no long-lived connection state that needs closing.
    }

    /**
     * @param array<string, mixed> $data
     */
    private function firstReceived(array $data, string $subscription): ?ReceivedMessage
    {
        $received = $data['receivedMessages'] ?? null;
        if ($received === null) {
            return null;
        }
        if (!is_array($received)) {
            throw new ProtocolException('Pub/Sub pull response field [receivedMessages] must be a list.');
        }
        $first = array_values($received)[0] ?? null;
        if ($first === null) {
            return null;
        }
        if (!is_array($first)) {
            throw new ProtocolException('Pub/Sub pull response contained a malformed received message.');
        }

        $ackId = $first['ackId'] ?? null;
        if (!is_string($ackId) || $ackId === '') {
            throw new ProtocolException('Pub/Sub pull response contained a message without an ack id.');
        }

        $message = $first['message'] ?? null;
        if (!is_array($message)) {
            throw new ProtocolException('Pub/Sub pull response contained a message without a payload object.');
        }

        $encodedData = $message['data'] ?? '';
        $body = is_string($encodedData) ? base64_decode($encodedData, true) : false;
        if ($body === false) {
            throw new ProtocolException('Pub/Sub message data is not valid base64.');
        }

        $headers = $message['attributes'] ?? [];
        if (!is_array($headers)) {
            throw new ProtocolException('Pub/Sub message attributes must be a map.');
        }

        $id = $message['messageId'] ?? null;
        $key = $message['orderingKey'] ?? null;

        if ($this->ackDeadlineSeconds > 0) {
            // Extend the lease past the subscription default so handlers have a known window.
            $this->modifyAckDeadline($subscription, $ackId, $this->ackDeadlineSeconds);
        }

        return new ReceivedMessage(
            body: $body,
            headers: $headers,
            id: is_string($id) ? $id : null,
            key: is_string($key) ? $key : null,
            // Pub/Sub only exposes delivery attempts with a dead-letter policy configured.
            attempts: 1,
            ackCallback: function () use ($subscription, $ackId): void {
                $this->acknowledge($subscription, $ackId);
            },
            rejectCallback: function (bool $requeue) use ($subscription, $ackId): void {
                if ($requeue) {
                    $this->modifyAckDeadline($subscription, $ackId, 0);
                } else {
                    $this->acknowledge($subscription, $ackId);
                }
            },
            raw: $first,
        );
    }

    private function acknowledge(string $subscription, string $ackId): void
    {
        $this->send(
            'POST',
            $this->subscriptionPath($subscription) . ':acknowledge',
            ['ackIds' => [$ackId]],
            $this->timeout,
        );
    }

    private function modifyAckDeadline(string $subscription, string $ackId, int $seconds): void
    {
        $this->send(
            'POST',
            $this->subscriptionPath($subscription) . ':modifyAckDeadline',
            ['ackIds' => [$ackId], 'ackDeadlineSeconds' => $seconds],
            $this->timeout,
        );
    }

    /**
     * @param array<string, mixed>|null $body
     * @return array<string, mixed>
     */
    private function send(string $method, string $path, ?array $body, float $timeout): array
    {
        /** @var array<string, mixed> $options */
        $options = [
            'headers' => ['Accept' => 'application/json'],
            'timeout' => $timeout,
            'http_errors' => false,
        ];
        if ($body !== null) {
            $options['json'] = $body;
        }
        if ($this->auth !== null) {
            $options['headers']['Authorization'] = 'Bearer ' . $this->auth->token();
        }

        try {
            $response = $this->http->request($method, $this->endpoint . $path, $options);
        } catch (ConnectException $exception) {
            if (($exception->getHandlerContext()['errno'] ?? null) === 28) {
                throw new TimeoutException('Pub/Sub request timed out.', 0, $exception);
            }

            throw new ConnectionException('Pub/Sub connection failed: ' . $exception->getMessage(), 0, $exception);
        } catch (GuzzleException $exception) {
            throw new ConnectionException('Pub/Sub request failed: ' . $exception->getMessage(), 0, $exception);
        }

        $status = $response->getStatusCode();
        $raw = (string) $response->getBody();
        $this->logger?->debug('Pub/Sub request completed.', [
            'method' => $method,
            'path' => $path,
            'status' => $status,
        ]);

        if ($status >= 400) {
            $this->throwHttpError($path, $status, $raw);
        }

        if ($raw === '') {
            return [];
        }

        $data = json_decode($raw, true);
        if (!is_array($data)) {
            throw new ProtocolException(sprintf('Pub/Sub response for [%s] was not a JSON object.', $path));
        }

        return $data;
    }

    private function throwHttpError(string $path, int $status, string $raw): void
    {
        $decoded = json_decode($raw, true);
        $error = is_array($decoded) && is_array($decoded['error'] ?? null) ? $decoded['error'] : [];
        $detail = is_string($error['message'] ?? null) && $error['message'] !== ''
            ? $error['message']
            : sprintf('HTTP %d', $status);

        $message = sprintf('Pub/Sub request [%s] failed with HTTP %d: %s', $path, $status, $detail);
        if ($status === 401 || $status === 403) {
            throw new AuthenticationException($message);
        }

        throw new RequestException($message, $status, $raw);
    }

    private function topicPath(string $topic): string
    {
        if ($topic === '') {
            throw new \InvalidArgumentException('Pub/Sub topic name must not be empty.');
        }

        return sprintf('/v1/projects/%s/topics/%s', rawurlencode($this->project), rawurlencode($topic));
    }

    private function subscriptionPath(string $subscription): string
    {
        return sprintf('/v1/projects/%s/subscriptions/%s', rawurlencode($this->project), rawurlencode($subscription));
    }

    /**
     * @param array<string, mixed> $headers
     * @return array<string, string>
     */
    private static function attributes(array $headers): array
    {
        $attributes = [];
        foreach ($headers as $name => $value) {
            if (!is_scalar($value)) {
                throw new UnsupportedFeatureException(sprintf(
                    'Pub/Sub attributes must be scalar; header [%s] has type [%s].',
                    (string) $name,
                    get_debug_type($value),
                ));
            }
            $attributes[(string) $name] = (string) $value;
        }

        return $attributes;
    }

    /**
     * @return array<string, mixed>
     */
    private static function loadCredentials(ConnectionConfig $config): array
    {
        $json = $config->get('credentials_json');
        if ($json === null) {
            $file = $config->get('credentials_file');
            if (!is_string($file) || $file === '') {
                throw new \InvalidArgumentException(sprintf(
                    'Connection [%s] requires [credentials_file] or [credentials_json] unless [emulator_host] is set.',
                    $config->name(),
                ));
            }
            if (!is_file($file) || !is_readable($file)) {
                throw new \InvalidArgumentException(sprintf(
                    'Connection [%s] option [credentials_file] [%s] is not readable.',
                    $config->name(),
                    $file,
                ));
            }

            $json = file_get_contents($file);
            if ($json === false) {
                throw new \InvalidArgumentException(sprintf(
                    'Connection [%s] option [credentials_file] [%s] could not be read.',
                    $config->name(),
                    $file,
                ));
            }
        }

        if (is_array($json)) {
            $credentials = $json;
        } elseif (is_string($json)) {
            try {
                $credentials = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
            } catch (\JsonException $exception) {
                throw new \InvalidArgumentException(sprintf(
                    'Connection [%s] service-account credentials are not valid JSON.',
                    $config->name(),
                ), 0, $exception);
            }
        } else {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [credentials_json] must be a JSON string or array.',
                $config->name(),
            ));
        }

        if (!is_array($credentials)) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] service-account credentials must be a JSON object.',
                $config->name(),
            ));
        }

        $type = $credentials['type'] ?? null;
        if ($type !== null && $type !== 'service_account') {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] credentials must be a service_account key, got [%s].',
                $config->name(),
                is_string($type) ? $type : get_debug_type($type),
            ));
        }

        foreach (['client_email', 'private_key'] as $field) {
            if (!is_string($credentials[$field] ?? null) || $credentials[$field] === '') {
                throw new \InvalidArgumentException(sprintf(
                    'Connection [%s] service-account credentials are missing [%s].',
                    $config->name(),
                    $field,
                ));
            }
        }

        return $credentials;
    }

    private static function emulatorEndpoint(string $host): string
    {
        return preg_match('~^https?://~i', $host) === 1 ? $host : 'http://' . $host;
    }
}
