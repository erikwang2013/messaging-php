<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Azure;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverFactoryInterface;
use Erikwang2013\Messaging\Contract\DriverInterface;
use GuzzleHttp\Client;
use GuzzleHttp\ClientInterface;
use Psr\Log\LoggerInterface;

/**
 * Builds Service Bus drivers from connection arrays.
 *
 * Required: shared_access_key_name, shared_access_key, and either namespace or endpoint,
 * plus a queue or a topic + subscription. A caller-supplied `client` (ClientInterface)
 * is accepted for tests and DI.
 */
final class ServiceBusFactory implements DriverFactoryInterface
{
    public function create(ConnectionConfig $config, ?LoggerInterface $logger = null): DriverInterface
    {
        $keyName = $config->requireString('shared_access_key_name');
        $key = $config->requireString('shared_access_key');

        $queue = $this->optionalString($config, 'queue');
        $topic = $this->optionalString($config, 'topic');
        $subscription = $this->optionalString($config, 'subscription');

        if ($queue !== null && ($topic !== null || $subscription !== null)) {
            throw self::invalid($config, 'Configure either [queue] or [topic] with [subscription], not both.');
        }

        if ($queue === null && $topic === null) {
            throw self::invalid($config, 'The connection requires a [queue] or a [topic] with [subscription].');
        }

        if (($topic === null) !== ($subscription === null)) {
            throw self::invalid($config, 'The [topic] and [subscription] options must be provided together.');
        }

        $timeout = $config->get('timeout', 30.0);
        if ((!is_int($timeout) && !is_float($timeout)) || $timeout <= 0) {
            throw self::invalid($config, 'The [timeout] option must be a positive number of seconds.');
        }

        $client = $config->get('client');
        if ($client !== null && !$client instanceof ClientInterface) {
            throw self::invalid($config, 'The [client] option must implement ' . ClientInterface::class . '.');
        }

        return new ServiceBusDriver(
            $client ?? new Client(),
            new SasToken($keyName, $key),
            $this->endpoint($config),
            $queue,
            $topic,
            $subscription,
            (float) $timeout,
            $logger,
        );
    }

    private function endpoint(ConnectionConfig $config): string
    {
        $endpoint = $config->get('endpoint');
        if ($endpoint !== null) {
            if (!is_string($endpoint) || trim($endpoint) === '') {
                throw self::invalid($config, 'The [endpoint] option must be a non-empty URL string.');
            }

            return self::normalizeEndpoint($config, $endpoint);
        }

        $namespace = $config->requireString('namespace');
        if (preg_match('/^[A-Za-z0-9][A-Za-z0-9-]*$/', $namespace) !== 1) {
            throw self::invalid($config, sprintf('Invalid Service Bus [namespace] [%s].', $namespace));
        }

        return 'https://' . strtolower($namespace) . '.servicebus.windows.net';
    }

    private static function normalizeEndpoint(ConnectionConfig $config, string $endpoint): string
    {
        $parts = parse_url(trim($endpoint));
        if (!is_array($parts) || !isset($parts['scheme'], $parts['host'])) {
            throw self::invalidEndpoint($config, $endpoint);
        }

        $scheme = strtolower($parts['scheme']);
        if (($scheme !== 'http' && $scheme !== 'https')
            || isset($parts['user'])
            || isset($parts['pass'])
            || isset($parts['query'])
            || isset($parts['fragment'])
        ) {
            throw self::invalidEndpoint($config, $endpoint);
        }

        $port = isset($parts['port']) ? ':' . $parts['port'] : '';

        return $scheme . '://' . strtolower($parts['host']) . $port . rtrim($parts['path'] ?? '', '/');
    }

    private static function invalidEndpoint(ConnectionConfig $config, string $endpoint): \InvalidArgumentException
    {
        return self::invalid($config, sprintf(
            'The [endpoint] option [%s] must be an http(s) URL without credentials, query or fragment.',
            $endpoint,
        ));
    }

    private function optionalString(ConnectionConfig $config, string $key): ?string
    {
        $value = $config->get($key);
        if ($value === null) {
            return null;
        }

        if (!is_string($value) || trim($value) === '') {
            throw self::invalid($config, sprintf('The [%s] option must be a non-empty string.', $key));
        }

        return trim($value);
    }

    private static function invalid(ConnectionConfig $config, string $message): \InvalidArgumentException
    {
        return new \InvalidArgumentException(sprintf('Connection [%s]: %s', $config->name(), $message));
    }
}
