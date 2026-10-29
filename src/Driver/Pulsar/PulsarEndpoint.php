<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Pulsar;

final class PulsarEndpoint
{
    private function __construct(
        private string $host,
        private int $port,
        private bool $tls,
    ) {
    }

    public static function fromUrl(string $url): self
    {
        if (strpos($url, ',') !== false) {
            throw new \InvalidArgumentException(
                'Pulsar service_url must contain a single broker; multiple host failover is not supported.',
            );
        }

        $parts = parse_url($url);
        if ($parts === false || !isset($parts['scheme'], $parts['host']) || $parts['host'] === '') {
            throw new \InvalidArgumentException(sprintf('Invalid Pulsar service_url [%s].', $url));
        }

        $scheme = strtolower($parts['scheme']);
        if ($scheme !== 'pulsar' && $scheme !== 'pulsar+ssl') {
            throw new \InvalidArgumentException(sprintf(
                'Pulsar service_url scheme must be pulsar:// or pulsar+ssl://, got [%s].',
                $scheme,
            ));
        }

        $path = $parts['path'] ?? '';
        if ($path !== '' && $path !== '/') {
            throw new \InvalidArgumentException('Pulsar service_url must not contain a path.');
        }

        if (isset($parts['user']) || isset($parts['pass']) || isset($parts['query']) || isset($parts['fragment'])) {
            throw new \InvalidArgumentException('Pulsar service_url must not contain credentials, query, or fragment.');
        }

        $tls = $scheme === 'pulsar+ssl';
        $port = $parts['port'] ?? ($tls ? 6651 : 6650);
        if (!is_int($port) || $port < 1 || $port > 65535) {
            throw new \InvalidArgumentException(sprintf('Invalid Pulsar service_url port [%s].', (string) $port));
        }

        return new self($parts['host'], $port, $tls);
    }

    public function host(): string
    {
        return $this->host;
    }

    public function port(): int
    {
        return $this->port;
    }

    public function tls(): bool
    {
        return $this->tls;
    }

    public function withTls(bool $tls): self
    {
        return new self($this->host, $this->port, $tls);
    }

    public function equals(self $other): bool
    {
        return $this->host === $other->host
            && $this->port === $other->port
            && $this->tls === $other->tls;
    }
}
