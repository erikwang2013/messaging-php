<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Kafka;

/**
 * Kafka classic consumer protocol metadata (subscription/assignment) and the range assignor.
 */
final class ConsumerProtocol
{
    /**
     * @param list<string> $topics
     */
    public static function encodeSubscription(array $topics): string
    {
        $encoder = (new Encoder())
            ->int16(0)
            ->int32(count($topics));

        foreach ($topics as $topic) {
            $encoder->string($topic);
        }

        return $encoder->bytes(null)->toString();
    }

    /**
     * @return list<string>
     */
    public static function decodeSubscription(string $metadata): array
    {
        $decoder = new Decoder($metadata);
        $decoder->int16(); // subscription version
        $count = $decoder->arrayLength() ?? 0;

        $topics = [];
        for ($i = 0; $i < $count; $i++) {
            $topics[] = $decoder->string();
        }

        return $topics;
    }

    /**
     * @param array<string, list<int>> $assignment
     */
    public static function encodeAssignment(array $assignment): string
    {
        $encoder = (new Encoder())
            ->int16(0)
            ->int32(count($assignment));

        foreach ($assignment as $topic => $partitions) {
            $encoder->string((string) $topic)->int32(count($partitions));
            foreach ($partitions as $partition) {
                $encoder->int32($partition);
            }
        }

        return $encoder->bytes(null)->toString();
    }

    /**
     * @return array<string, list<int>>
     */
    public static function decodeAssignment(string $assignment): array
    {
        $decoder = new Decoder($assignment);
        $decoder->int16(); // assignment version
        $topicCount = $decoder->arrayLength() ?? 0;

        $result = [];
        for ($i = 0; $i < $topicCount; $i++) {
            $topic = $decoder->string();
            $partitionCount = $decoder->arrayLength() ?? 0;
            $partitions = [];
            for ($p = 0; $p < $partitionCount; $p++) {
                $partitions[] = $decoder->int32();
            }

            sort($partitions);
            $result[$topic] = $partitions;
        }

        return $result;
    }

    /**
     * Kafka's range assignor: members and partitions are sorted, each topic is split evenly.
     *
     * @param array<string, list<string>> $subscriptions member id => subscribed topics
     * @param array<string, int> $partitionCounts topic => partition count
     * @return array<string, array<string, list<int>>> member id => topic => partitions
     */
    public static function rangeAssign(array $subscriptions, array $partitionCounts): array
    {
        $members = array_keys($subscriptions);
        sort($members);

        $byTopic = [];
        foreach ($subscriptions as $member => $topics) {
            foreach ($topics as $topic) {
                $byTopic[$topic][(string) $member] = true;
            }
        }

        $assignment = [];
        foreach ($members as $member) {
            $assignment[(string) $member] = [];
        }

        ksort($byTopic);
        foreach ($byTopic as $topic => $interested) {
            $topicMembers = array_keys($interested);
            sort($topicMembers);

            $count = $partitionCounts[$topic] ?? 0;
            $memberCount = count($topicMembers);
            if ($memberCount === 0 || $count === 0) {
                continue;
            }

            $base = intdiv($count, $memberCount);
            $extra = $count % $memberCount;
            $partition = 0;

            foreach ($topicMembers as $index => $member) {
                $take = $base + ($index < $extra ? 1 : 0);
                if ($take === 0) {
                    continue;
                }

                $assignment[$member][$topic] = range($partition, $partition + $take - 1);
                $partition += $take;
            }
        }

        return $assignment;
    }
}
