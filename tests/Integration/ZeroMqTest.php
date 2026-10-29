<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Integration;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Driver\ZeroMq\ZeroMqFactory;
use Erikwang2013\Messaging\Tests\Contract\DriverContractTestCase;
use PHPUnit\Framework\Attributes\Group;

/**
 * ZeroMQ is brokerless: these tests need an external ZMTP peer that serves the
 * configured role (PUB for a subscriber, SUB for a publisher, or a relay that
 * accepts both). Without one, every test is skipped.
 */
#[Group('integration')]
final class ZeroMqTest extends DriverContractTestCase
{
    protected function createDriver(): DriverInterface
    {
        $host = getenv('MESSAGING_ZEROMQ_HOST');
        $host = is_string($host) && $host !== '' ? $host : '127.0.0.1';

        $port = getenv('MESSAGING_ZEROMQ_PORT');
        $port = is_string($port) && $port !== '' ? (int) $port : 5555;

        $pattern = getenv('MESSAGING_ZEROMQ_PATTERN');
        $pattern = is_string($pattern) && $pattern !== '' ? $pattern : 'pubsub';

        $probe = @stream_socket_client(
            sprintf('tcp://%s:%d', $host, $port),
            $errorCode,
            $errorMessage,
            0.5,
        );
        if ($probe === false) {
            self::markTestSkipped(sprintf(
                'No ZeroMQ peer is listening at %s:%d (%s).',
                $host,
                $port,
                $errorMessage !== '' ? $errorMessage : 'connection failed',
            ));
        }
        fclose($probe);

        return (new ZeroMqFactory())->create(ConnectionConfig::fromArray('zeromq', [
            'driver' => 'zeromq',
            'host' => $host,
            'port' => $port,
            'pattern' => $pattern,
            'timeout' => 2.0,
        ]));
    }
}
