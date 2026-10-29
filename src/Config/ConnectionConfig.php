<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Config;

final class ConnectionConfig
{
    /**
     * @param array<string, mixed> $options
     */
    private function __construct(
        private readonly string $name,
        private readonly string $driver,
        private readonly array $options,
    ) {
    }

    /**
     * @param array<string, mixed> $config
     */
    public static function fromArray(string $name, array $config): self
    {
        $driver = $config['driver'] ?? null;
        if (!is_string($driver) || $driver === '') {
            throw new \InvalidArgumentException(sprintf('Connection [%s] requires a non-empty [driver] option.', $name));
        }

        unset($config['driver']);

        return new self($name, $driver, $config);
    }

    public function name(): string
    {
        return $this->name;
    }

    public function driver(): string
    {
        return $this->driver;
    }

    /**
     * @return array<string, mixed>
     */
    public function options(): array
    {
        return $this->options;
    }

    public function get(string $key, mixed $default = null): mixed
    {
        return $this->options[$key] ?? $default;
    }

    public function requireString(string $key): string
    {
        $value = $this->options[$key] ?? null;
        if (!is_string($value) || $value === '') {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] requires a non-empty string option [%s].',
                $this->name,
                $key,
            ));
        }

        return $value;
    }
}
