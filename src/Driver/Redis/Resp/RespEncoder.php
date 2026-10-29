<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Redis\Resp;

final class RespEncoder
{
    /**
     * @param list<string|int> $arguments
     */
    public static function encode(array $arguments): string
    {
        $encoded = '*' . count($arguments) . "\r\n";

        foreach ($arguments as $argument) {
            $value = (string) $argument;
            $encoded .= '$' . strlen($value) . "\r\n" . $value . "\r\n";
        }

        return $encoded;
    }
}
