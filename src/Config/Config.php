<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Config;

final class Config
{
    /**
     * @param array<string, ConnectionConfig> $connections
     */
    private function __construct(
        private readonly array $connections,
        private readonly string $defaultConnection,
    ) {
    }

    /**
     * @param array<string, mixed> $config
     */
    public static function fromArray(array $config): self
    {
        $rawConnections = $config['connections'] ?? null;
        if (!is_array($rawConnections) || $rawConnections === []) {
            throw new \InvalidArgumentException('The [connections] option must be a non-empty array.');
        }

        $connections = [];
        foreach ($rawConnections as $name => $connection) {
            if (!is_array($connection)) {
                throw new \InvalidArgumentException(sprintf('Connection [%s] must be an array.', $name));
            }

            $connections[(string) $name] = ConnectionConfig::fromArray((string) $name, $connection);
        }

        $default = $config['default'] ?? array_key_first($connections);
        if (!is_string($default) || !isset($connections[$default])) {
            throw new \InvalidArgumentException(sprintf(
                'Default connection [%s] is not defined.',
                is_string($default) ? $default : get_debug_type($default),
            ));
        }

        return new self($connections, $default);
    }

    public function defaultConnection(): string
    {
        return $this->defaultConnection;
    }

    public function connection(string $name): ConnectionConfig
    {
        if (!isset($this->connections[$name])) {
            throw new \InvalidArgumentException(sprintf('Connection [%s] is not defined.', $name));
        }

        return $this->connections[$name];
    }
}
