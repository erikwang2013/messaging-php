<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\RocketMq;

use Erikwang2013\Messaging\Exception\ProtocolException;

/**
 * Normalized TopicRouteData returned by a nameserver.
 */
final class RocketMqRoute
{
    private function __construct()
    {
    }

    /**
     * @return array{
     *     brokers: array<string, string>,
     *     queues: list<array{broker: string, readQueueNums: int, writeQueueNums: int, perm: int}>
     * }
     */
    public static function parse(string $json): array
    {
        try {
            $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $exception) {
            throw new ProtocolException('Invalid RocketMQ route JSON: ' . $exception->getMessage(), 0, $exception);
        }

        if (!is_array($data)) {
            throw new ProtocolException('RocketMQ route payload must be a JSON object.');
        }

        $brokerDatas = $data['brokerDatas'] ?? [];
        if (!is_array($brokerDatas)) {
            throw new ProtocolException('RocketMQ route field [brokerDatas] must be an array.');
        }

        $brokers = [];
        foreach ($brokerDatas as $brokerData) {
            if (!is_array($brokerData)) {
                throw new ProtocolException('RocketMQ route entry in [brokerDatas] must be an object.');
            }

            $brokerName = $brokerData['brokerName'] ?? null;
            $brokerAddrs = $brokerData['brokerAddrs'] ?? null;
            if (!is_string($brokerName) || $brokerName === '' || !is_array($brokerAddrs)) {
                throw new ProtocolException('RocketMQ route broker entry requires [brokerName] and [brokerAddrs].');
            }

            // json_decode turns the numeric broker id key "0" into integer 0.
            $master = $brokerAddrs[0] ?? $brokerAddrs['0'] ?? null;
            if (is_string($master) && $master !== '') {
                $brokers[$brokerName] = $master;
            }
        }

        $queueDatas = $data['queueDatas'] ?? [];
        if (!is_array($queueDatas)) {
            throw new ProtocolException('RocketMQ route field [queueDatas] must be an array.');
        }

        $queues = [];
        foreach ($queueDatas as $queueData) {
            if (!is_array($queueData)) {
                throw new ProtocolException('RocketMQ route entry in [queueDatas] must be an object.');
            }

            $brokerName = $queueData['brokerName'] ?? null;
            $readQueueNums = $queueData['readQueueNums'] ?? null;
            $writeQueueNums = $queueData['writeQueueNums'] ?? null;
            $perm = $queueData['perm'] ?? null;
            if (
                !is_string($brokerName)
                || !is_int($readQueueNums)
                || !is_int($writeQueueNums)
                || !is_int($perm)
            ) {
                throw new ProtocolException(
                    'RocketMQ route queue entry requires [brokerName], [readQueueNums], [writeQueueNums] and [perm].',
                );
            }

            $queues[] = [
                'broker' => $brokerName,
                'readQueueNums' => $readQueueNums,
                'writeQueueNums' => $writeQueueNums,
                'perm' => $perm,
            ];
        }

        return ['brokers' => $brokers, 'queues' => $queues];
    }
}
