<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Aws;

use Erikwang2013\Messaging\Config\ConnectionConfig;

/**
 * Common configuration for the AWS drivers.
 */
final class AwsConfig
{
    public function __construct(
        public string $region,
        public string $accessKey,
        public string $secretKey,
        public ?string $sessionToken,
        public ?string $endpoint,
        public float $timeout,
    ) {
    }

    public static function fromConnectionConfig(ConnectionConfig $config): self
    {
        $region = $config->get('region');
        if (!is_string($region) || trim($region) === '') {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires a non-empty [region] option.',
                $config->name(),
            ));
        }

        $accessKey = self::optionOrEnvironment($config, 'access_key', 'AWS_ACCESS_KEY_ID');
        $secretKey = self::optionOrEnvironment($config, 'secret_key', 'AWS_SECRET_ACCESS_KEY');
        if ($accessKey === null || $secretKey === null) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires [access_key] and [secret_key] options or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY environment variables.',
                $config->name(),
            ));
        }

        $sessionToken = self::optionalString($config, 'session_token') ?? self::environment('AWS_SESSION_TOKEN');
        $endpoint = self::optionalString($config, 'endpoint');
        if ($endpoint !== null) {
            $parts = parse_url($endpoint);
            $scheme = is_array($parts) && isset($parts['scheme']) ? strtolower($parts['scheme']) : null;
            if (!is_array($parts) || !isset($parts['host']) || !in_array($scheme, ['http', 'https'], true)) {
                throw new \InvalidArgumentException(sprintf(
                    'Connection [%s] option [endpoint] must be an http or https URL.',
                    $config->name(),
                ));
            }

            $endpoint = rtrim($endpoint, '/');
        }

        $timeout = $config->get('timeout', 30.0);
        if ((!is_int($timeout) && !is_float($timeout)) || $timeout <= 0) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [timeout] must be a positive number.',
                $config->name(),
            ));
        }

        return new self($region, $accessKey, $secretKey, $sessionToken, $endpoint, (float) $timeout);
    }

    private static function optionOrEnvironment(ConnectionConfig $config, string $key, string $environment): ?string
    {
        $value = $config->get($key);
        if ($value !== null) {
            if (!is_string($value) || $value === '') {
                throw new \InvalidArgumentException(sprintf(
                    'Connection [%s] option [%s] must be a non-empty string.',
                    $config->name(),
                    $key,
                ));
            }

            return $value;
        }

        return self::environment($environment);
    }

    private static function optionalString(ConnectionConfig $config, string $key): ?string
    {
        $value = $config->get($key);
        if ($value === null) {
            return null;
        }

        if (!is_string($value)) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [%s] must be a string.',
                $config->name(),
                $key,
            ));
        }

        $value = trim($value);

        return $value === '' ? null : $value;
    }

    private static function environment(string $name): ?string
    {
        $value = getenv($name);

        return is_string($value) && $value !== '' ? $value : null;
    }
}
