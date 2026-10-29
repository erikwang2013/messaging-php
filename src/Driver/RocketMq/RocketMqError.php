<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RocketMq;

use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\RequestException;

final class RocketMqError
{
    private function __construct()
    {
    }

    public static function throwFor(RocketMqCommand $command, string $action): void
    {
        $code = $command->code();
        if ($code === RocketMqProtocol::SUCCESS) {
            return;
        }

        $remark = $command->remark();
        $message = sprintf('RocketMQ %s failed with code [%d]', $action, $code);
        if ($remark !== null && $remark !== '') {
            $message .= ': ' . $remark;
        }

        if ($code === RocketMqProtocol::NO_PERMISSION) {
            throw new AuthenticationException($message, $code);
        }

        throw new RequestException($message, $code, $remark);
    }
}
