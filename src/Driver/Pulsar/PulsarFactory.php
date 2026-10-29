<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Pulsar;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverFactoryInterface;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Psr\Log\LoggerInterface;

final class PulsarFactory implements DriverFactoryInterface
{
    public function create(ConnectionConfig $config, ?LoggerInterface $logger = null): DriverInterface
    {
        $endpoint = PulsarEndpoint::fromUrl($config->requireString('service_url'));

        $tls = $config->get('tls', false);
        if (!is_bool($tls)) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [tls] must be a boolean.',
                $config->name(),
            ));
        }

        if ($tls) {
            $endpoint = $endpoint->withTls(true);
        }

        $tlsContext = $config->get('tls_context', []);
        if (!is_array($tlsContext)) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [tls_context] must be an array.',
                $config->name(),
            ));
        }

        $topic = $config->get('topic');
        if ($topic !== null && (!is_string($topic) || $topic === '')) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [topic] must be a non-empty string.',
                $config->name(),
            ));
        }

        $subscription = $config->get('subscription', 'messaging');
        if (!is_string($subscription) || $subscription === '') {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [subscription] must be a non-empty string.',
                $config->name(),
            ));
        }

        $consumerName = $config->get('consumer_name');
        if ($consumerName !== null && (!is_string($consumerName) || $consumerName === '')) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [consumer_name] must be a non-empty string.',
                $config->name(),
            ));
        }

        $timeout = $config->get('timeout', 10.0);
        if ((!is_int($timeout) && !is_float($timeout)) || $timeout <= 0) {
            throw new \InvalidArgumentException(sprintf(
                'Connection [%s] option [timeout] must be a positive number.',
                $config->name(),
            ));
        }

        return new PulsarDriver(
            $endpoint,
            $topic,
            $subscription,
            $consumerName,
            (float) $timeout,
            $tlsContext,
            $logger,
        );
    }
}
