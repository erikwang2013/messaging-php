<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Integration;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Driver\Beanstalkd\BeanstalkdFactory;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Support\Socket;
use Erikwang2013\Messaging\Tests\Contract\DriverContractTestCase;
use PHPUnit\Framework\Attributes\Group;

#[Group('integration')]
final class BeanstalkdTest extends DriverContractTestCase
{
    protected function createDriver(): DriverInterface
    {
        $host = getenv('MESSAGING_BEANSTALKD_HOST') ?: '127.0.0.1';
        $port = (int) (getenv('MESSAGING_BEANSTALKD_PORT') ?: 11300);

        try {
            $probe = Socket::connect($host, $port, 1.0);
            $probe->close();
        } catch (ConnectionException $exception) {
            self::markTestSkipped(sprintf(
                'Beanstalkd is not reachable at %s:%d: %s',
                $host,
                $port,
                $exception->getMessage(),
            ));
        }

        return (new BeanstalkdFactory())->create(ConnectionConfig::fromArray('beanstalkd', [
            'driver' => 'beanstalkd',
            'host' => $host,
            'port' => $port,
        ]));
    }
}
