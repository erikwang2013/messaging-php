<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\RocketMq;

use Erikwang2013\Messaging\Driver\RocketMq\RocketMqCommand;
use Erikwang2013\Messaging\Driver\RocketMq\RocketMqError;
use Erikwang2013\Messaging\Driver\RocketMq\RocketMqProtocol;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\RequestException;
use PHPUnit\Framework\TestCase;

final class RocketMqErrorTest extends TestCase
{
    public function test_success_does_not_throw(): void
    {
        RocketMqError::throwFor($this->command(RocketMqProtocol::SUCCESS), 'send message');

        self::assertTrue(true);
    }

    public function test_broker_errors_become_request_exceptions(): void
    {
        try {
            RocketMqError::throwFor($this->command(RocketMqProtocol::TOPIC_NOT_EXIST, 'topic not exist'), 'send message');
            self::fail('Expected a RequestException.');
        } catch (RequestException $exception) {
            self::assertSame(RocketMqProtocol::TOPIC_NOT_EXIST, $exception->statusCode());
            self::assertSame('topic not exist', $exception->responseBody());
            self::assertStringContainsString('send message', $exception->getMessage());
            self::assertStringContainsString('topic not exist', $exception->getMessage());
        }
    }

    public function test_permission_errors_become_authentication_exceptions(): void
    {
        $this->expectException(AuthenticationException::class);

        RocketMqError::throwFor($this->command(RocketMqProtocol::NO_PERMISSION), 'send message');
    }

    private function command(int $code, ?string $remark = null): RocketMqCommand
    {
        return new RocketMqCommand($code, 1, 1, $remark, [], '');
    }
}
