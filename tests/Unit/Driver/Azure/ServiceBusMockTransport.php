<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Azure;

use GuzzleHttp\Promise\Create;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\Response;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

/**
 * In-memory Azure Service Bus data-plane fake used as a Guzzle handler.
 *
 * It understands the subset of the REST API the driver uses: send, peek-lock,
 * complete (DELETE) and abandon (PUT), including SAS validation and scheduling.
 */
final class ServiceBusMockTransport
{
    /** @var array<string, list<array<string, mixed>>> */
    private array $messages = [];

    private int $locks = 0;

    public function __construct(
        private string $keyName,
        private string $key,
    ) {
    }

    public function __invoke(RequestInterface $request): PromiseInterface
    {
        return Create::promiseFor($this->handle($request));
    }

    private function handle(RequestInterface $request): ResponseInterface
    {
        if (!$this->authorized($request->getHeaderLine('Authorization'))) {
            return new Response(401, [], 'SAS authentication failed.');
        }

        $path = rawurldecode($request->getUri()->getPath());

        if (str_contains($path, 'missing')) {
            return new Response(410, [], 'The queue or subscription does not exist.');
        }

        if (preg_match('~^/(.+)/messages$~', $path, $matches) === 1) {
            return $request->getMethod() === 'POST'
                ? $this->send($matches[1], $request)
                : new Response(405);
        }

        if (preg_match('~^/(.+)/messages/head$~', $path, $matches) === 1) {
            return $request->getMethod() === 'POST'
                ? $this->receive($matches[1], $request)
                : new Response(405);
        }

        if (preg_match('~^/(.+)/messages/([^/]+)/([^/]+)$~', $path, $matches) === 1) {
            return $this->settle(
                $matches[1],
                rawurldecode($matches[2]),
                rawurldecode($matches[3]),
                $request->getMethod(),
            );
        }

        return new Response(404);
    }

    private function send(string $entity, RequestInterface $request): ResponseInterface
    {
        $properties = json_decode($request->getHeaderLine('BrokerProperties'), true);
        if (!is_array($properties)) {
            return new Response(400, [], 'BrokerProperties must be a JSON object.');
        }

        /** @var array<string, mixed> $properties */
        $id = $properties['MessageId'] ?? null;
        if (!is_string($id) || $id === '') {
            return new Response(400, [], 'MessageId is required.');
        }

        $scheduled = $properties['ScheduledEnqueueTimeUtc'] ?? null;
        $visibleAt = is_string($scheduled) ? (int) strtotime($scheduled) : 0;

        $custom = [];
        foreach ($request->getHeaders() as $name => $values) {
            if (self::isReservedHeader($name)) {
                continue;
            }

            $custom[$name] = $values[0] ?? '';
        }

        $this->messages[$entity][] = [
            'id' => $id,
            'properties' => $properties,
            'body' => (string) $request->getBody(),
            'content_type' => $request->getHeaderLine('Content-Type'),
            'custom' => $custom,
            'visible_at' => $visibleAt,
            'delivery' => 0,
            'lock' => null,
        ];

        return new Response(201);
    }

    private function receive(string $entity, RequestInterface $request): ResponseInterface
    {
        $now = time();

        foreach ($this->messages[$entity] ?? [] as $index => $message) {
            if ($message['lock'] !== null || $message['visible_at'] > $now) {
                continue;
            }

            $lock = 'lock-' . (++$this->locks);
            $delivery = $message['delivery'] + 1;
            $this->messages[$entity][$index]['lock'] = $lock;
            $this->messages[$entity][$index]['delivery'] = $delivery;

            /** @var array<string, mixed> $properties */
            $properties = $message['properties'];
            $properties['LockToken'] = $lock;
            $properties['DeliveryCount'] = $delivery;
            $properties['LockedUntilUtc'] = gmdate('D, d M Y H:i:s \G\M\T', $now + 60);
            $properties['SequenceNumber'] = $index + 1;

            $location = $request->getUri()->getScheme() . '://' . $request->getUri()->getAuthority()
                . '/' . $entity . '/messages/' . rawurlencode($message['id']) . '/' . rawurlencode($lock);

            $headers = [
                'BrokerProperties' => (string) json_encode($properties, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES),
                'Location' => $location,
                'Content-Type' => $message['content_type'],
            ] + $message['custom'];

            return new Response(201, $headers, $message['body']);
        }

        return new Response(204);
    }

    private function settle(string $entity, string $id, string $token, string $method): ResponseInterface
    {
        foreach ($this->messages[$entity] ?? [] as $index => $message) {
            if ($message['id'] !== $id || $message['lock'] !== $token) {
                continue;
            }

            if ($method === 'DELETE') {
                unset($this->messages[$entity][$index]);
                $this->messages[$entity] = array_values($this->messages[$entity]);
            } elseif ($method === 'PUT') {
                $this->messages[$entity][$index]['lock'] = null;
            } else {
                return new Response(405);
            }

            return new Response(200);
        }

        return new Response(404, [], 'No message was found for the specified lock token.');
    }

    private function authorized(string $header): bool
    {
        $prefix = 'SharedAccessSignature ';
        if (!str_starts_with($header, $prefix)) {
            return false;
        }

        $parameters = [];
        foreach (explode('&', substr($header, strlen($prefix))) as $part) {
            $pair = explode('=', $part, 2);
            if (count($pair) === 2) {
                $parameters[$pair[0]] = $pair[1];
            }
        }

        $resource = $parameters['sr'] ?? '';
        $signature = urldecode($parameters['sig'] ?? '');
        $expires = (int) ($parameters['se'] ?? 0);
        $keyName = urldecode($parameters['skn'] ?? '');

        if ($resource === '' || $keyName !== $this->keyName || $expires <= time()) {
            return false;
        }

        $expected = base64_encode(hash_hmac('sha256', $resource . "\n" . $expires, $this->key, true));

        return hash_equals($expected, $signature);
    }

    private static function isReservedHeader(string $name): bool
    {
        $normalized = strtolower((string) preg_replace('/[^A-Za-z0-9]/', '', $name));

        return in_array($normalized, [
            'authorization',
            'brokerproperties',
            'contenttype',
            'contentlength',
            'host',
            'useragent',
            'accept',
            'acceptencoding',
            'connection',
            'expect',
        ], true) || str_starts_with($normalized, 'xms');
    }
}
