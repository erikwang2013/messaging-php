<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

use Erikwang2013\Messaging\Config\ConnectionConfig;

/**
 * Validated Kafka driver configuration.
 */
final class KafkaConfig
{
    /** @var array<string, int> */
    public const ALIASES = [
        'kafka' => 9092,
        'redpanda' => 9092,
        'automq' => 9092,
        'confluent' => 9092,
        'eventhubs' => 9093,
    ];

    /**
     * @param list<array{host: string, port: int}> $brokers
     * @param array<string, mixed> $tlsContext
     */
    public function __construct(
        public array $brokers,
        public string $groupId = 'messaging',
        public string $clientId = 'messaging-php',
        public float $timeout = 10.0,
        public bool $tls = false,
        public array $tlsContext = [],
        public string $autoOffsetReset = 'latest',
        public int $acks = 1,
        public int $fetchMaxWaitMs = 500,
        public int $sessionTimeoutMs = 10000,
        public int $maxPollRecords = 1,
        public string $alias = 'kafka',
    ) {
    }

    public static function fromConnection(ConnectionConfig $config, string $alias = 'kafka'): self
    {
        if (!isset(self::ALIASES[$alias])) {
            throw new \InvalidArgumentException(sprintf('Unknown Kafka driver alias [%s].', $alias));
        }

        $brokers = self::parseBrokers($config->get('brokers'), self::ALIASES[$alias], $config->name());

        $groupId = $config->get('group_id', 'messaging');
        if (!is_string($groupId) || $groupId === '') {
            throw new \InvalidArgumentException(sprintf('Connection [%s] option [group_id] must be a non-empty string.', $config->name()));
        }

        $clientId = $config->get('client_id', 'messaging-php');
        if (!is_string($clientId) || $clientId === '') {
            throw new \InvalidArgumentException(sprintf('Connection [%s] option [client_id] must be a non-empty string.', $config->name()));
        }

        $timeout = $config->get('timeout', 10.0);
        if (!self::isPositiveNumber($timeout)) {
            throw new \InvalidArgumentException(sprintf('Connection [%s] option [timeout] must be a positive number.', $config->name()));
        }

        $tls = $config->get('tls', false);
        if (!is_bool($tls)) {
            throw new \InvalidArgumentException(sprintf('Connection [%s] option [tls] must be a boolean.', $config->name()));
        }

        $tlsContext = $config->get('tls_context', []);
        if (!is_array($tlsContext)) {
            throw new \InvalidArgumentException(sprintf('Connection [%s] option [tls_context] must be an array.', $config->name()));
        }

        $autoOffsetReset = $config->get('auto_offset_reset', 'latest');
        if (!in_array($autoOffsetReset, ['earliest', 'latest'], true)) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [auto_offset_reset] must be [earliest] or [latest].',
                $config->name(),
            ));
        }

        $acks = $config->get('acks', 1);
        if (!is_int($acks) || !in_array($acks, [1, -1], true)) {
            throw new \InvalidArgumentException(sprintf('Connection [%s] option [acks] must be 1 or -1 (all).', $config->name()));
        }

        $fetchMaxWait = $config->get('fetch_max_wait_ms', 500);
        if (!is_int($fetchMaxWait) || $fetchMaxWait < 0) {
            throw new \InvalidArgumentException(sprintf('Connection [%s] option [fetch_max_wait_ms] must be a non-negative integer.', $config->name()));
        }

        $sessionTimeout = $config->get('session_timeout_ms', 10000);
        if (!is_int($sessionTimeout) || $sessionTimeout < 1) {
            throw new \InvalidArgumentException(sprintf('Connection [%s] option [session_timeout_ms] must be a positive integer.', $config->name()));
        }

        $maxPollRecords = $config->get('max_poll_records', 1);
        if (!is_int($maxPollRecords) || $maxPollRecords < 1) {
            throw new \InvalidArgumentException(sprintf('Connection [%s] option [max_poll_records] must be a positive integer.', $config->name()));
        }

        return new self(
            $brokers,
            $groupId,
            $clientId,
            (float) $timeout,
            $tls,
            $tlsContext,
            $autoOffsetReset,
            $acks,
            $fetchMaxWait,
            $sessionTimeout,
            $maxPollRecords,
            $alias,
        );
    }

    /**
     * @return list<array{host: string, port: int}>
     */
    private static function parseBrokers(mixed $value, int $defaultPort, string $connection): array
    {
        if (is_string($value)) {
            $value = array_map('trim', explode(',', $value));
        }

        if (!is_array($value) || $value === []) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires a non-empty [brokers] array of host:port entries.',
                $connection,
            ));
        }

        $brokers = [];
        foreach ($value as $entry) {
            if (!is_string($entry) || trim($entry) === '') {
                throw new \InvalidArgumentException(sprintf(
                    'Connection [%s] option [brokers] must contain non-empty strings.',
                    $connection,
                ));
            }

            $entry = trim($entry);
            $host = $entry;
            $port = $defaultPort;

            if (str_starts_with($entry, '[')) {
                $end = strpos($entry, ']');
                if ($end === false) {
                    throw new \InvalidArgumentException(sprintf('Invalid Kafka broker address [%s].', $entry));
                }

                $host = substr($entry, 1, $end - 1);
                $suffix = substr($entry, $end + 1);
                if ($suffix !== '') {
                    if (!str_starts_with($suffix, ':')) {
                        throw new \InvalidArgumentException(sprintf('Invalid Kafka broker address [%s].', $entry));
                    }

                    $port = (int) substr($suffix, 1);
                }
            } elseif (substr_count($entry, ':') === 1) {
                [$host, $portString] = explode(':', $entry);
                $port = (int) $portString;
            }

            if ($host === '' || $port < 1 || $port > 65535) {
                throw new \InvalidArgumentException(sprintf('Invalid Kafka broker address [%s].', $entry));
            }

            $brokers[] = ['host' => $host, 'port' => $port];
        }

        return $brokers;
    }

    private static function isPositiveNumber(mixed $value): bool
    {
        return (is_int($value) || is_float($value)) && $value > 0;
    }
}
