<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Contract;

use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Tests\Contract\DriverContractTestCase;
use Erikwang2013\Messaging\Tests\Support\FakeDriver;

final class FakeDriverContractTest extends DriverContractTestCase
{
    protected function createDriver(): DriverInterface
    {
        return new FakeDriver();
    }
}
