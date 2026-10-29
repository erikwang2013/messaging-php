<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Azure;

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
use GuzzleHttp\ClientInterface;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\TransferException;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;

/**
 * Azure Service Bus data-plane REST driver (SAS authentication, peek-lock receive).
 *
 * Endpoints used (no api-version is required for message operations):
 * - send:     POST   {endpoint}/{queue|topic}/messages
 * - receive:  POST   {endpoint}/{queue|topic/subscriptions/sub}/messages/head?timeout={seconds}
 * - complete: DELETE {Location returned by the peek-lock response}
 * - abandon:  PUT    {Location returned by the peek-lock response}
 *
 * The settle URI is taken from the Location header unchanged, as required by:
 * https://learn.microsoft.com/en-us/rest/api/servicebus/delete-message
 * https://learn.microsoft.com/en-us/rest/api/servicebus/unlock-message
 *
 * Message::key maps to BrokerProperties.CorrelationId in both directions.
 * ContentType (or Content-Type) in Message::headers sets the HTTP Content-Type; it is
 * surfaced on receive through raw()['content_type'] rather than as a custom header.
 */
final class ServiceBusDriver implements DriverInterface
{
    private const HTTP_DATE = 'D, d M Y H:i:s \G\M\T';

    /** @var array<string, string> */
    private const SYSTEM_HEADERS = [
        'messageid' => 'MessageId',
        'label' => 'Label',
        'correlationid' => 'CorrelationId',
        'replyto' => 'ReplyTo',
        'replytosessionid' => 'ReplyToSessionId',
        'to' => 'To',
        'sessionid' => 'SessionId',
        'partitionkey' => 'PartitionKey',
        'timetolive' => 'TimeToLive',
        'scheduledenqueuetimeutc' => 'ScheduledEnqueueTimeUtc',
    ];

    /** @var list<string> */
    private const RESERVED_REQUEST_HEADERS = [
        'authorization',
        'brokerproperties',
        'contenttype',
        'contentlength',
        'host',
        'expect',
        'connection',
        'transferencoding',
        'date',
        'useragent',
        'accept',
        'acceptencoding',
        'xmsretrypolicy',
    ];

    /** @var list<string> */
    private const RESERVED_RESPONSE_HEADERS = [
        'brokerproperties',
        'location',
        'contenttype',
        'contentlength',
        'date',
        'server',
        'transferencoding',
        'connection',
        'keepalive',
        'etag',
        'via',
        'retryafter',
        'stricttransportsecurity',
        'setcookie',
    ];

    private string $endpoint;
    private string $defaultSendEntity;
    private string $defaultReceiveEntity;

    public function __construct(
        private ClientInterface $client,
        private SasToken $sas,
        string $endpoint,
        ?string $queue = null,
        ?string $topic = null,
        ?string $subscription = null,
        private float $timeout = 30.0,
        private ?LoggerInterface $logger = null,
    ) {
        if ($timeout <= 0) {
            throw new \InvalidArgumentException('The Azure Service Bus timeout must be positive.');
        }

        $this->endpoint = rtrim($endpoint, '/');
        if ($this->endpoint === '') {
            throw new \InvalidArgumentException('The Azure Service Bus endpoint cannot be empty.');
        }

        if ($queue !== null) {
            if ($topic !== null || $subscription !== null) {
                throw new \InvalidArgumentException('Configure either [queue] or [topic] with [subscription], not both.');
            }

            $this->defaultSendEntity = self::assertEntityPath($queue);
            $this->defaultReceiveEntity = $this->defaultSendEntity;

            return;
        }

        if ($topic === null || $subscription === null) {
            throw new \InvalidArgumentException('Azure Service Bus requires [queue] or [topic] with [subscription].');
        }

        $this->defaultSendEntity = self::assertEntityPath($topic);
        $this->defaultReceiveEntity = $this->defaultSendEntity . '/subscriptions/' . self::assertEntityName($subscription);
    }

    public function publish(string $destination, Message $message, array $options = []): PublishResult
    {
        $entity = $this->sendEntityPath($destination);
        [$properties, $headers, $contentType] = $this->buildSendHeaders($message);

        $response = $this->request(
            'send',
            'POST',
            $this->endpoint . '/' . $entity . '/messages',
            $this->resourceUri($entity),
            ['Content-Type' => $contentType, 'BrokerProperties' => self::encodeBrokerProperties($properties)] + $headers,
            $message->body,
        );

        $id = $properties['MessageId'];

        return new PublishResult($destination, is_string($id) ? $id : null, [
            'status' => $response->getStatusCode(),
            'broker_properties' => $properties,
        ]);
    }

    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        throw new UnsupportedFeatureException(
            'The Azure Service Bus REST driver sends messages individually and does not implement batch publishing.',
        );
    }

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage
    {
        $entity = $this->receiveEntityPath($destination);
        $resource = $this->resourceUri($entity);
        $waitSeconds = $this->receiveTimeoutSeconds($options->timeoutMs);

        $response = $this->request(
            'receive',
            'POST',
            $this->endpoint . '/' . $entity . '/messages/head?timeout=' . $waitSeconds,
            $resource,
            [],
            '',
            max($this->timeout, $waitSeconds + 5.0),
        );

        if ($response->getStatusCode() === 204) {
            return null;
        }

        $properties = $this->parseBrokerProperties($response);
        $lockToken = self::stringValue($properties['LockToken'] ?? null);
        if ($lockToken === null) {
            throw new ProtocolException('Azure Service Bus peek-lock response is missing BrokerProperties.LockToken.');
        }

        $messageId = self::stringValue($properties['MessageId'] ?? null);
        $sequenceNumber = self::stringValue($properties['SequenceNumber'] ?? null);
        $reference = $messageId ?? $sequenceNumber;
        if ($reference === null) {
            throw new ProtocolException('Azure Service Bus response is missing BrokerProperties.MessageId.');
        }

        $location = $response->getHeaderLine('Location');
        $settleUri = $location !== ''
            ? $location
            : $this->endpoint . '/' . $entity . '/messages/' . rawurlencode($reference) . '/' . rawurlencode($lockToken);

        $deliveryCount = $properties['DeliveryCount'] ?? null;
        $attempts = is_int($deliveryCount) && $deliveryCount > 0 ? $deliveryCount : 1;

        return new ReceivedMessage(
            (string) $response->getBody(),
            $this->customHeaders($response),
            $messageId ?? $reference,
            self::stringValue($properties['CorrelationId'] ?? null),
            $attempts,
            function () use ($settleUri, $resource): void {
                $this->settle('complete', 'DELETE', $settleUri, $resource);
            },
            function (bool $requeue) use ($settleUri, $resource): void {
                $this->settle($requeue ? 'abandon' : 'complete', $requeue ? 'PUT' : 'DELETE', $settleUri, $resource);
            },
            [
                'broker_properties' => $properties,
                'location' => $location,
                'status' => $response->getStatusCode(),
                'content_type' => $response->getHeaderLine('Content-Type'),
            ],
        );
    }

    public function capabilities(): Capabilities
    {
        return new Capabilities(
            ack: true,
            requeue: true,
            delay: true,
            consumerGroups: false,
            batchPublish: false,
            ordering: false,
            headers: true,
        );
    }

    public function close(): void
    {
        // Guzzle clients are stateless; leases and locks are owned by the broker.
    }

    /**
     * @return array{0: array<string, mixed>, 1: array<string, string>, 2: string}
     */
    private function buildSendHeaders(Message $message): array
    {
        $properties = ['MessageId' => self::generateMessageId()];
        $headers = [];
        $contentType = 'text/plain';
        $scheduled = false;

        foreach ($message->headers as $name => $value) {
            if (!is_string($name) || $name === '') {
                throw new \InvalidArgumentException('Azure Service Bus header names must be non-empty strings.');
            }

            $normalized = self::normalizeHeaderName($name);
            if ($normalized === 'contenttype') {
                $contentType = self::headerString($name, $value);
                continue;
            }

            $property = self::SYSTEM_HEADERS[$normalized] ?? null;
            if ($property !== null) {
                if ($property === 'TimeToLive') {
                    if (!is_int($value) || $value <= 0) {
                        throw new \InvalidArgumentException(sprintf('Header [%s] must be a positive integer of seconds.', $name));
                    }

                    $properties['TimeToLive'] = $value;
                } elseif ($property === 'ScheduledEnqueueTimeUtc') {
                    $properties['ScheduledEnqueueTimeUtc'] = self::formatHttpDate($name, $value);
                    $scheduled = true;
                } else {
                    $properties[$property] = self::headerString($name, $value);
                }

                continue;
            }

            if (self::isReservedRequestHeader($normalized)) {
                throw new \InvalidArgumentException(sprintf(
                    'Header [%s] is reserved by the Azure Service Bus REST API; use Message::key or a system header instead.',
                    $name,
                ));
            }

            if (!preg_match('/^[A-Za-z0-9!#$%&\'*+.^_`|~-]+$/', $name)) {
                throw new \InvalidArgumentException(sprintf('Header [%s] is not a valid HTTP header name.', $name));
            }

            $headers[$name] = self::encodeHeaderValue($name, $value);
        }

        if ($message->key !== null) {
            $existing = $properties['CorrelationId'] ?? null;
            if ($existing !== null && $existing !== $message->key) {
                throw new \InvalidArgumentException(
                    'Message::key and the [CorrelationId] header disagree; set only one of them.',
                );
            }

            $properties['CorrelationId'] = $message->key;
        }

        if ($message->delayMs !== null) {
            if ($scheduled) {
                throw new \InvalidArgumentException(
                    'Use either Message::delayMs or the [ScheduledEnqueueTimeUtc] header, not both.',
                );
            }

            if ($message->delayMs > 0) {
                // Service Bus schedules with one-second resolution.
                $properties['ScheduledEnqueueTimeUtc'] = gmdate(
                    self::HTTP_DATE,
                    time() + (int) ceil($message->delayMs / 1000),
                );
            }
        }

        return [$properties, $headers, $contentType];
    }

    private function sendEntityPath(string $destination): string
    {
        $destination = trim($destination);
        if ($destination === '') {
            return $this->defaultSendEntity;
        }

        $entity = self::assertEntityPath($destination);
        if (str_contains($entity, '/subscriptions/')) {
            throw new \InvalidArgumentException('Messages are published to a queue or topic, not to a subscription path.');
        }

        return $entity;
    }

    private function receiveEntityPath(string $destination): string
    {
        $destination = trim($destination);

        return $destination === '' ? $this->defaultReceiveEntity : self::assertEntityPath($destination);
    }

    private function resourceUri(string $entity): string
    {
        return $this->endpoint . '/' . $entity;
    }

    private function receiveTimeoutSeconds(int $timeoutMs): int
    {
        if ($timeoutMs === 0) {
            return max(1, (int) ceil($this->timeout));
        }

        return max(1, (int) ceil($timeoutMs / 1000));
    }

    /**
     * @param array<string, string> $headers
     */
    private function request(
        string $operation,
        string $method,
        string $uri,
        string $resource,
        array $headers,
        string $body = '',
        ?float $timeout = null,
    ): ResponseInterface {
        $timeout ??= $this->timeout;
        $headers['Authorization'] = $this->sas->forResource($resource);

        try {
            $response = $this->client->request($method, $uri, [
                'headers' => $headers,
                'body' => $body,
                'timeout' => $timeout,
                'connect_timeout' => min(10.0, $timeout),
                'http_errors' => false,
                'allow_redirects' => false,
            ]);
        } catch (ConnectException $exception) {
            throw $this->connectionError($operation, $exception);
        } catch (TransferException $exception) {
            throw new ConnectionException(
                sprintf('Azure Service Bus %s request failed: %s', $operation, $exception->getMessage()),
                0,
                $exception,
            );
        }

        $status = $response->getStatusCode();
        $this->logger?->debug('Azure Service Bus request completed', [
            'operation' => $operation,
            'method' => $method,
            'resource' => $resource,
            'status' => $status,
        ]);

        if ($status < 200 || $status >= 300) {
            $payload = (string) $response->getBody();
            $hint = '';
            if ($status === 404) {
                $hint = in_array($operation, ['complete', 'abandon'], true)
                    ? ' (message not found or lock lost)'
                    : ' (queue or topic not found)';
            } elseif ($status === 410) {
                $hint = ' (queue or subscription does not exist)';
            }

            $message = sprintf(
                'Azure Service Bus %s failed with HTTP %d%s%s',
                $operation,
                $status,
                $hint,
                $payload === '' ? '.' : ': ' . substr($payload, 0, 1000),
            );

            if ($status === 401 || $status === 403) {
                throw new AuthenticationException($message);
            }

            throw new RequestException($message, $status, $payload);
        }

        return $response;
    }

    private function settle(string $operation, string $method, string $uri, string $resource): void
    {
        $this->request($operation, $method, $uri, $resource, [], '');
    }

    /**
     * @return array<string, mixed>
     */
    private function parseBrokerProperties(ResponseInterface $response): array
    {
        $raw = $response->getHeaderLine('BrokerProperties');
        if ($raw === '') {
            throw new ProtocolException('Azure Service Bus response is missing the BrokerProperties header.');
        }

        try {
            $decoded = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $exception) {
            throw new ProtocolException('Malformed BrokerProperties header: ' . $raw, 0, $exception);
        }

        if (!is_array($decoded)) {
            throw new ProtocolException('BrokerProperties must decode to a JSON object.');
        }

        /** @var array<string, mixed> $decoded */
        return $decoded;
    }

    /**
     * @return array<string, mixed>
     */
    private function customHeaders(ResponseInterface $response): array
    {
        $headers = [];

        foreach ($response->getHeaders() as $name => $values) {
            if (self::isReservedResponseHeader($name)) {
                continue;
            }

            $headers[$name] = self::decodeHeaderValue($values[0] ?? '');
        }

        return $headers;
    }

    private function connectionError(string $operation, ConnectException $exception): ConnectionException
    {
        // cURL error 28 is CURLE_OPERATION_TIMEOUTED.
        $errno = $exception->getHandlerContext()['errno'] ?? null;
        if ($errno === 28 || stripos($exception->getMessage(), 'timed out') !== false) {
            return new TimeoutException(
                sprintf('Azure Service Bus %s request timed out: %s', $operation, $exception->getMessage()),
                0,
                $exception,
            );
        }

        return new ConnectionException(
            sprintf('Azure Service Bus %s connection failed: %s', $operation, $exception->getMessage()),
            0,
            $exception,
        );
    }

    private static function assertEntityPath(string $path): string
    {
        $path = trim($path, '/');
        $segments = explode('/', $path);

        if (count($segments) === 1) {
            return self::assertEntityName($segments[0]);
        }

        if (count($segments) === 3 && strcasecmp($segments[1], 'subscriptions') === 0) {
            return self::assertEntityName($segments[0]) . '/subscriptions/' . self::assertEntityName($segments[2]);
        }

        throw new \InvalidArgumentException(sprintf(
            'Invalid Azure Service Bus destination [%s]: use [queue] or [topic/subscriptions/subscription].',
            $path,
        ));
    }

    private static function assertEntityName(string $name): string
    {
        if (!preg_match('/^[A-Za-z0-9][A-Za-z0-9._~-]*$/', $name)) {
            throw new \InvalidArgumentException(sprintf('Invalid Azure Service Bus entity name [%s].', $name));
        }

        return $name;
    }

    private static function normalizeHeaderName(string $name): string
    {
        return strtolower((string) preg_replace('/[^A-Za-z0-9]/', '', $name));
    }

    private static function isReservedRequestHeader(string $normalizedName): bool
    {
        return in_array($normalizedName, self::RESERVED_REQUEST_HEADERS, true)
            || str_starts_with($normalizedName, 'xms');
    }

    private static function isReservedResponseHeader(string $name): bool
    {
        $normalized = self::normalizeHeaderName($name);

        return in_array($normalized, self::RESERVED_RESPONSE_HEADERS, true)
            || str_starts_with($normalized, 'xms');
    }

    private static function generateMessageId(): string
    {
        return bin2hex(random_bytes(16));
    }

    private static function headerString(string $name, mixed $value): string
    {
        if (!is_string($value) || $value === '') {
            throw new \InvalidArgumentException(sprintf('Header [%s] must be a non-empty string.', $name));
        }

        if (preg_match('/[\r\n\0]/', $value)) {
            throw new \InvalidArgumentException(sprintf('Header [%s] contains invalid control characters.', $name));
        }

        return $value;
    }

    private static function encodeHeaderValue(string $name, mixed $value): string
    {
        if (is_bool($value)) {
            return $value ? 'true' : 'false';
        }

        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }

        if (!is_string($value)) {
            throw new \InvalidArgumentException(sprintf(
                'Header [%s] must be a scalar string, number or boolean; arrays and objects are not supported.',
                $name,
            ));
        }

        if (preg_match('/[\r\n\0]/', $value)) {
            throw new \InvalidArgumentException(sprintf('Header [%s] contains invalid control characters.', $name));
        }

        // Service Bus infers unquoted values as booleans/numbers; quote strings to keep them strings.
        return (string) json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
    }

    private static function decodeHeaderValue(string $value): mixed
    {
        $value = trim($value);

        if (strlen($value) >= 2 && $value[0] === '"' && str_ends_with($value, '"')) {
            $decoded = json_decode($value, true);
            if (is_string($decoded)) {
                return $decoded;
            }
        }

        if ($value === 'true') {
            return true;
        }

        if ($value === 'false') {
            return false;
        }

        if (preg_match('/^-?(0|[1-9][0-9]*)$/', $value) === 1) {
            $integer = (int) $value;

            return (string) $integer === $value ? $integer : $value;
        }

        if (is_numeric($value)) {
            return (float) $value;
        }

        return $value;
    }

    /**
     * @param array<string, mixed> $properties
     */
    private static function encodeBrokerProperties(array $properties): string
    {
        return (string) json_encode($properties, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
    }

    private static function formatHttpDate(string $name, mixed $value): string
    {
        if ($value instanceof \DateTimeInterface) {
            return gmdate(self::HTTP_DATE, $value->getTimestamp());
        }

        if (is_int($value)) {
            return gmdate(self::HTTP_DATE, $value);
        }

        if (is_string($value) && $value !== '') {
            $timestamp = strtotime($value);
            if ($timestamp !== false) {
                return gmdate(self::HTTP_DATE, $timestamp);
            }
        }

        throw new \InvalidArgumentException(sprintf(
            'Header [%s] must be a date/time string, UNIX timestamp or DateTimeInterface.',
            $name,
        ));
    }

    private static function stringValue(mixed $value): ?string
    {
        if (is_string($value) && $value !== '') {
            return $value;
        }

        if (is_int($value)) {
            return (string) $value;
        }

        return null;
    }
}
