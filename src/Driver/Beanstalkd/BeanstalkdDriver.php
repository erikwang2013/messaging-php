<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Beanstalkd;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Erikwang2013\Messaging\Support\Socket;

final class BeanstalkdDriver implements DriverInterface
{
    /**
     * Beanstalkd tube names are at most 200 bytes of ASCII letters, digits and -+/;.$_().
     */
    private const TUBE_PATTERN = '~\A[A-Za-z0-9+/;.$_()-]{1,200}\z~';

    private string $host;
    private int $port;
    private float $connectTimeout;
    private int $priority;
    private int $ttr;

    private ?Socket $socket = null;
    private ?string $usingTube = null;

    /** @var list<string> */
    private array $watched = ['default'];

    public function __construct(ConnectionConfig $config)
    {
        $this->host = $config->requireString('host');
        $this->port = self::intOption($config, 'port', 11300, 1, 65535);
        $this->priority = self::intOption($config, 'priority', 1024, 0, 4294967295);
        $this->ttr = self::intOption($config, 'ttr', 60, 0, 4294967295);
        $this->connectTimeout = self::floatOption($config, 'timeout', 5.0);
    }

    public function publish(string $destination, Message $message, array $options = []): PublishResult
    {
        self::assertTube($destination);

        if ($message->headers !== []) {
            throw new UnsupportedFeatureException('Beanstalkd does not support message headers.');
        }

        if ($message->key !== null) {
            throw new UnsupportedFeatureException('Beanstalkd does not support message keys.');
        }

        $protocol = $this->protocol();
        if ($this->usingTube !== $destination) {
            $protocol->useTube($destination);
            $this->usingTube = $destination;
        }

        $id = $protocol->put(
            $this->priority,
            BeanstalkdProtocol::delaySeconds($message->delayMs),
            $this->ttr,
            $message->body,
        );

        return new PublishResult($destination, (string) $id, $id);
    }

    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        throw new UnsupportedFeatureException('Beanstalkd does not support batch publishing.');
    }

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage
    {
        self::assertTube($destination);

        $protocol = $this->protocol();
        $this->watchOnly($protocol, $destination);

        $reserved = $protocol->reserve(BeanstalkdProtocol::timeoutSeconds($options->timeoutMs));
        if ($reserved === null) {
            return null;
        }

        $id = $reserved['id'];
        $defaultPriority = $this->priority;

        return new ReceivedMessage(
            body: $reserved['body'],
            id: (string) $id,
            ackCallback: static function () use ($protocol, $id): void {
                $protocol->delete($id);
            },
            rejectCallback: static function (bool $requeue) use ($protocol, $id, $defaultPriority): void {
                if (!$requeue) {
                    $protocol->delete($id);

                    return;
                }

                $stats = $protocol->statsJob($id);
                $priority = isset($stats['pri']) && preg_match('/\A\d{1,10}\z/', $stats['pri']) === 1
                    ? (int) $stats['pri']
                    : $defaultPriority;

                $protocol->release($id, $priority, 0);
            },
            raw: ['id' => $id, 'tube' => $destination],
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
            headers: false,
        );
    }

    public function close(): void
    {
        $this->socket?->close();
        $this->socket = null;
        $this->usingTube = null;
        $this->watched = ['default'];
    }

    private function protocol(): BeanstalkdProtocol
    {
        return new BeanstalkdProtocol(
            $this->socket ??= Socket::connect($this->host, $this->port, $this->connectTimeout),
            $this->connectTimeout,
        );
    }

    private function watchOnly(BeanstalkdProtocol $protocol, string $tube): void
    {
        if ($this->watched === [$tube]) {
            return;
        }

        // Watch first so the connection never ends up with zero watched tubes.
        $protocol->watch($tube);
        $this->watched[] = $tube;

        foreach ($this->watched as $watched) {
            if ($watched !== $tube) {
                $protocol->ignore($watched);
            }
        }

        $this->watched = [$tube];
    }

    private static function assertTube(string $tube): void
    {
        if (preg_match(self::TUBE_PATTERN, $tube) !== 1) {
            throw new \InvalidArgumentException(sprintf('Invalid Beanstalkd tube name [%s].', $tube));
        }
    }

    private static function intOption(ConnectionConfig $config, string $key, int $default, int $min, int $max): int
    {
        $value = $config->get($key, $default);
        if (!is_int($value) || $value < $min || $value > $max) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [%s] must be an integer between %d and %d.',
                $config->name(),
                $key,
                $min,
                $max,
            ));
        }

        return $value;
    }

    private static function floatOption(ConnectionConfig $config, string $key, float $default): float
    {
        $value = $config->get($key, $default);
        if ((!is_int($value) && !is_float($value)) || $value <= 0) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [%s] must be a positive number.',
                $config->name(),
                $key,
            ));
        }

        return (float) $value;
    }
}
