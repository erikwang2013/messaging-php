<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RocketMq;

use Erikwang2013\Messaging\Config\ConnectionConfig;

final class RocketMqConfig
{
    public const DEFAULT_PORT = 9876;
    public const DEFAULT_BROKER_PORT = 10911;

    /** Topic and consumer group name charset accepted by RocketMQ. */
    private const NAME_PATTERN = '/\A[%|a-zA-Z0-9_-]{1,127}\z/';

    private function __construct(
        private string $nameserverHost,
        private int $nameserverPort,
        private string $group,
        private ?string $defaultTopic,
        private float $timeout,
        private bool $consumeFromFirst,
        private int $pullBatchSize,
    ) {
    }

    public static function fromConnection(ConnectionConfig $config): self
    {
        [$host, $port] = self::parseAddress($config->requireString('nameserver'), self::DEFAULT_PORT);

        $group = $config->get('group', 'messaging');
        if (!is_string($group) || preg_match(self::NAME_PATTERN, $group) !== 1) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [group] must be a valid RocketMQ name (%s).',
                $config->name(),
                self::NAME_PATTERN,
            ));
        }

        $topic = $config->get('topic');
        if ($topic !== null && (!is_string($topic) || preg_match(self::NAME_PATTERN, $topic) !== 1)) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [topic] must be a valid RocketMQ topic name.',
                $config->name(),
            ));
        }

        $timeout = $config->get('timeout', 10.0);
        if (!(is_int($timeout) || is_float($timeout)) || $timeout <= 0) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [timeout] must be a positive number.',
                $config->name(),
            ));
        }

        $consumeFrom = $config->get('consume_from', 'last');
        if ($consumeFrom !== 'last' && $consumeFrom !== 'first') {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [consume_from] must be [last] or [first].',
                $config->name(),
            ));
        }

        $pullBatchSize = $config->get('pull_batch_size', 1);
        if (!is_int($pullBatchSize) || $pullBatchSize < 1) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [pull_batch_size] must be a positive integer.',
                $config->name(),
            ));
        }

        return new self(
            $host,
            $port,
            $group,
            $topic,
            (float) $timeout,
            $consumeFrom === 'first',
            $pullBatchSize,
        );
    }

    /**
     * Parses [host[:port]], [host:port], [ipv6]:port and [ipv6] addresses.
     *
     * @return array{0: string, 1: int}
     */
    public static function parseAddress(string $address, int $defaultPort): array
    {
        $address = trim($address);
        if ($address === '') {
            throw new \InvalidArgumentException('RocketMQ address must not be empty.');
        }

        if ($address[0] === '[') {
            $end = strpos($address, ']');
            if ($end === false) {
                throw new \InvalidArgumentException(sprintf('Invalid bracketed RocketMQ address [%s].', $address));
            }

            $host = substr($address, 1, $end - 1);
            $rest = substr($address, $end + 1);
            if ($rest === '') {
                return [self::assertHost($host, $address), $defaultPort];
            }
            if ($rest[0] !== ':') {
                throw new \InvalidArgumentException(sprintf('Invalid bracketed RocketMQ address [%s].', $address));
            }
            $portPart = substr($rest, 1);
        } else {
            $colon = strpos($address, ':');
            if ($colon === false) {
                return [self::assertHost($address, $address), $defaultPort];
            }
            if (strpos($address, ':', $colon + 1) !== false) {
                throw new \InvalidArgumentException(sprintf(
                    'IPv6 RocketMQ addresses must be bracketed, got [%s].',
                    $address,
                ));
            }
            $host = substr($address, 0, $colon);
            $portPart = substr($address, $colon + 1);
        }

        if (preg_match('/\A\d{1,5}\z/', $portPart) !== 1) {
            throw new \InvalidArgumentException(sprintf('Invalid RocketMQ port in address [%s].', $address));
        }

        $port = (int) $portPart;
        if ($port < 1 || $port > 65535) {
            throw new \InvalidArgumentException(sprintf('RocketMQ port out of range in address [%s].', $address));
        }

        return [self::assertHost($host, $address), $port];
    }

    public function nameserverHost(): string
    {
        return $this->nameserverHost;
    }

    public function nameserverPort(): int
    {
        return $this->nameserverPort;
    }

    public function group(): string
    {
        return $this->group;
    }

    public function timeout(): float
    {
        return $this->timeout;
    }

    public function consumeFromFirst(): bool
    {
        return $this->consumeFromFirst;
    }

    public function pullBatchSize(): int
    {
        return $this->pullBatchSize;
    }

    public function destination(string $destination): string
    {
        if ($destination === '') {
            if ($this->defaultTopic === null) {
                throw new \InvalidArgumentException('RocketMQ requires a non-empty destination or a [topic] option.');
            }

            return $this->defaultTopic;
        }

        if (preg_match(self::NAME_PATTERN, $destination) !== 1) {
            throw new \InvalidArgumentException(sprintf(
                'Invalid RocketMQ topic name [%s]; expected %s.',
                $destination,
                self::NAME_PATTERN,
            ));
        }

        return $destination;
    }

    private static function assertHost(string $host, string $address): string
    {
        if ($host === '') {
            throw new \InvalidArgumentException(sprintf('RocketMQ address [%s] is missing a host.', $address));
        }

        return $host;
    }
}
