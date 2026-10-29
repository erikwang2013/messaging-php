<?php

declare(strict_types=1);

require dirname(__DIR__, 4) . '/vendor/autoload.php';

use Erikwang2013\Messaging\Driver\Kafka\Decoder;
use Erikwang2013\Messaging\Driver\Kafka\Encoder;

$logFile = $argv[1] ?? null;
$mode = $argv[2] ?? 'basic';

$server = @stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
if ($server === false) {
    fwrite(STDERR, sprintf("Unable to listen: %s (%d)\n", $errorMessage, $errorCode));
    exit(1);
}

$address = (string) stream_socket_get_name($server, false);
$port = (int) substr($address, strrpos($address, ':') + 1);
$host = '127.0.0.1';

/** @var array<string, int> $partitionCounts */
$partitionCounts = ['orders' => 2, 'events' => 2];

/** @var array<string, array<int, int>> $ends */
$ends = [];

/** @var array<string, array<int, list<string>>> $batches */
$batches = [];

/** @var array<string, array<string, array<int, int>>> $committed */
$committed = [];

$apiNames = [
    0 => 'Produce',
    1 => 'Fetch',
    2 => 'ListOffsets',
    3 => 'Metadata',
    8 => 'OffsetCommit',
    9 => 'OffsetFetch',
    10 => 'FindCoordinator',
    11 => 'JoinGroup',
    12 => 'Heartbeat',
    13 => 'LeaveGroup',
    14 => 'SyncGroup',
    18 => 'ApiVersions',
];

$log = static function (string $api, int $version) use ($logFile): void {
    if ($logFile !== null) {
        file_put_contents(
            $logFile,
            json_encode(['api' => $api, 'version' => $version]) . "\n",
            FILE_APPEND | LOCK_EX,
        );
    }
};

$readExactly = static function ($connection, int $length): ?string {
    $buffer = '';
    while (strlen($buffer) < $length) {
        $chunk = fread($connection, $length - strlen($buffer));
        if ($chunk === false || $chunk === '') {
            return null;
        }

        $buffer .= $chunk;
    }

    return $buffer;
};

$send = static function ($connection, int $correlationId, Encoder $body): bool {
    $payload = (new Encoder())->int32($correlationId)->raw($body->toString())->toString();

    return fwrite($connection, pack('N', strlen($payload)) . $payload) !== false;
};

$ensureTopic = static function (string $topic) use (&$partitionCounts, &$ends): void {
    if (!isset($partitionCounts[$topic])) {
        $partitionCounts[$topic] = 2;
    }

    for ($partition = 0; $partition < $partitionCounts[$topic]; $partition++) {
        $ends[$topic][$partition] ??= 0;
    }
};

echo 'PORT ' . $port . "\n";
fflush(STDOUT);

while (true) {
    $connection = @stream_socket_accept($server, -1);
    if ($connection === false) {
        continue;
    }

    stream_set_timeout($connection, 30);

    while (true) {
        $sizeBytes = $readExactly($connection, 4);
        if ($sizeBytes === null) {
            break;
        }

        $size = unpack('N', $sizeBytes)[1];
        if ($size < 8 || $size > 104_857_600) {
            break;
        }

        $frame = $readExactly($connection, $size);
        if ($frame === null) {
            break;
        }

        $decoder = new Decoder($frame);
        $apiKey = $decoder->int16();
        $version = $decoder->int16();
        $correlationId = $decoder->int32();

        if ($apiKey === 18 && $version >= 3) {
            $decoder->compactNullableString();
            $decoder->skipTags();
        } else {
            $decoder->nullableString();
        }

        $apiName = $apiNames[$apiKey] ?? ('Api' . $apiKey);
        $log($apiName, $version);

        if ($mode === 'malformed' && $apiKey !== 18) {
            fwrite($connection, pack('N', 3) . 'abc');
            break;
        }

        try {
            switch ($apiKey) {
                case 18: // ApiVersions
                    if ($mode === 'force_v3' && $version === 0) {
                        $body = (new Encoder())->int16(35)->int32(0);
                        break;
                    }

                    $versions = [
                        [0, 0, $mode === 'old_version' ? 2 : 3],
                        [1, 0, 4],
                        [2, 0, 1],
                        [3, 0, 4],
                        [8, 0, 2],
                        [9, 0, 2],
                        [10, 0, 1],
                        [11, 0, 2],
                        [12, 0, 1],
                        [13, 0, 2],
                        [14, 0, 2],
                        [18, 0, 3],
                    ];

                    $body = (new Encoder())->int16(0);
                    if ($version >= 3) {
                        $body->uvarint(count($versions) + 1);
                        foreach ($versions as [$key, $min, $max]) {
                            $body->int16($key)->int16($min)->int16($max)->uvarint(0);
                        }

                        $body->int32(0)->uvarint(0);
                    } else {
                        $body->int32(count($versions));
                        foreach ($versions as [$key, $min, $max]) {
                            $body->int16($key)->int16($min)->int16($max);
                        }
                    }
                    break;

                case 3: // Metadata
                    $requested = [];
                    $topicCount = $decoder->int32();
                    for ($i = 0; $i < $topicCount; $i++) {
                        $requested[] = $decoder->string();
                    }
                    $decoder->bool();

                    $topics = $requested === [] ? array_keys($partitionCounts) : $requested;
                    foreach ($topics as $topic) {
                        $ensureTopic($topic);
                    }

                    $body = (new Encoder())
                        ->int32(0)
                        ->int32(1)
                        ->int32(1)
                        ->string($host)
                        ->int32($port)
                        ->nullableString(null)
                        ->nullableString('fake-cluster')
                        ->int32(1)
                        ->int32(count($topics));

                    foreach ($topics as $topic) {
                        $body
                            ->int16(0)
                            ->string($topic)
                            ->bool(false)
                            ->int32($partitionCounts[$topic]);

                        for ($partition = 0; $partition < $partitionCounts[$topic]; $partition++) {
                            $body
                                ->int16(0)
                                ->int32($partition)
                                ->int32(1)
                                ->int32(1)->int32(1)
                                ->int32(1)->int32(1);
                        }
                    }
                    break;

                case 0: // Produce
                    $decoder->nullableString();
                    $decoder->int16();
                    $decoder->int32();

                    $results = [];
                    $topicCount = $decoder->int32();
                    for ($i = 0; $i < $topicCount; $i++) {
                        $topic = $decoder->string();
                        $ensureTopic($topic);

                        $partitionCount = $decoder->int32();
                        for ($p = 0; $p < $partitionCount; $p++) {
                            $partition = $decoder->int32();
                            $batch = $decoder->bytes() ?? '';
                            $lastOffsetDelta = strlen($batch) >= 27
                                ? unpack('N', substr($batch, 23, 4))[1]
                                : 0;

                            $base = $ends[$topic][$partition] ?? 0;
                            if (strlen($batch) >= 8) {
                                $batch = substr_replace($batch, (new Encoder())->int64($base)->toString(), 0, 8);
                            }

                            $batches[$topic][$partition][] = $batch;
                            $ends[$topic][$partition] = $base + $lastOffsetDelta + 1;
                            $results[$topic][$partition] = $base;
                        }
                    }

                    $body = (new Encoder())->int32(count($results));
                    foreach ($results as $topic => $partitions) {
                        $body->string((string) $topic)->int32(count($partitions));
                        foreach ($partitions as $partition => $base) {
                            $body
                                ->int32($partition)
                                ->int16(0)
                                ->int64($base)
                                ->int64(-1);
                        }
                    }
                    $body->int32(0);
                    break;

                case 1: // Fetch
                    $decoder->int32();
                    $maxWaitMs = $decoder->int32();
                    $decoder->int32();
                    $decoder->int32();
                    $decoder->int8();

                    $requested = [];
                    $topicCount = $decoder->int32();
                    for ($i = 0; $i < $topicCount; $i++) {
                        $topic = $decoder->string();
                        $ensureTopic($topic);

                        $partitionCount = $decoder->int32();
                        for ($p = 0; $p < $partitionCount; $p++) {
                            $partition = $decoder->int32();
                            $offset = $decoder->int64();
                            $decoder->int32();
                            $requested[$topic][$partition] = $offset;
                        }
                    }

                    $hasRecords = false;
                    $body = (new Encoder())->int32(0)->int32(count($requested));
                    foreach ($requested as $topic => $partitions) {
                        $body->string((string) $topic)->int32(count($partitions));
                        foreach ($partitions as $partition => $offset) {
                            $end = $ends[$topic][$partition] ?? 0;
                            $records = '';
                            if ($offset < $end) {
                                foreach ($batches[$topic][$partition] ?? [] as $batch) {
                                    $records .= $batch;
                                }
                                $hasRecords = $records !== '';
                            }

                            $body
                                ->int32($partition)
                                ->int16(0)
                                ->int64($end)
                                ->int64($end)
                                ->int32(-1)
                                ->bytes($records);
                        }
                    }

                    if (!$hasRecords && $maxWaitMs > 0) {
                        usleep(min($maxWaitMs, 50) * 1000);
                    }
                    break;

                case 2: // ListOffsets
                    $decoder->int32(); // replica id

                    $requested = [];
                    $topicCount = $decoder->int32();
                    for ($i = 0; $i < $topicCount; $i++) {
                        $topic = $decoder->string();
                        $ensureTopic($topic);

                        $partitionCount = $decoder->int32();
                        for ($p = 0; $p < $partitionCount; $p++) {
                            $partition = $decoder->int32();
                            $timestamp = $decoder->int64();
                            $requested[$topic][$partition] = $timestamp;
                        }
                    }

                    $body = (new Encoder())->int32(count($requested));
                    foreach ($requested as $topic => $partitions) {
                        $body->string((string) $topic)->int32(count($partitions));
                        foreach ($partitions as $partition => $timestamp) {
                            $offset = $timestamp === -2 ? 0 : ($ends[$topic][$partition] ?? 0);
                            $body
                                ->int32($partition)
                                ->int16(0)
                                ->int64(-1)
                                ->int64($offset);
                        }
                    }
                    break;

                case 10: // FindCoordinator
                    $decoder->string();
                    $decoder->int8();

                    $body = (new Encoder())
                        ->int32(0)
                        ->int16(0)
                        ->nullableString(null)
                        ->int32(1)
                        ->string($host)
                        ->int32($port);
                    break;

                case 11: // JoinGroup
                    $decoder->string();
                    $decoder->int32();
                    $decoder->int32();
                    $decoder->string();
                    $decoder->string();

                    $protocolName = 'range';
                    $metadata = '';
                    $protocolCount = $decoder->int32();
                    for ($i = 0; $i < $protocolCount; $i++) {
                        $name = $decoder->string();
                        $protocolMetadata = $decoder->bytes() ?? '';
                        if ($i === 0) {
                            $protocolName = $name;
                            $metadata = $protocolMetadata;
                        }
                    }

                    $body = (new Encoder())
                        ->int32(0)
                        ->int16(0)
                        ->int32(1)
                        ->string($protocolName)
                        ->string('member-1')
                        ->string('member-1')
                        ->int32(1)
                        ->string('member-1')
                        ->bytes($metadata);
                    break;

                case 14: // SyncGroup
                    $decoder->string();
                    $decoder->int32();
                    $memberId = $decoder->string();

                    $assignments = [];
                    $assignmentCount = $decoder->int32();
                    for ($i = 0; $i < $assignmentCount; $i++) {
                        $member = $decoder->string();
                        $assignments[$member] = $decoder->bytes() ?? '';
                    }

                    $assignment = $assignments[$memberId] ?? ($assignments === [] ? '' : reset($assignments));

                    $body = (new Encoder())->int32(0)->int16(0)->bytes($assignment);
                    break;

                case 12: // Heartbeat
                case 13: // LeaveGroup
                    $decoder->string();
                    if ($apiKey === 12) {
                        $decoder->int32();
                    }
                    $decoder->string();

                    $body = (new Encoder())->int32(0)->int16(0);
                    break;

                case 8: // OffsetCommit
                    $groupId = $decoder->string();
                    $decoder->int32();
                    $decoder->string();
                    $decoder->int64();

                    $offsets = [];
                    $topicCount = $decoder->int32();
                    for ($i = 0; $i < $topicCount; $i++) {
                        $topic = $decoder->string();
                        $partitionCount = $decoder->int32();
                        for ($p = 0; $p < $partitionCount; $p++) {
                            $partition = $decoder->int32();
                            $offset = $decoder->int64();
                            $decoder->nullableString();
                            $committed[$groupId][$topic][$partition] = $offset;
                            $offsets[$topic][$partition] = true;
                        }
                    }

                    $body = (new Encoder())->int32(count($offsets));
                    foreach ($offsets as $topic => $partitions) {
                        $body->string((string) $topic)->int32(count($partitions));
                        foreach (array_keys($partitions) as $partition) {
                            $body->int32($partition)->int16(0);
                        }
                    }
                    break;

                case 9: // OffsetFetch
                    $groupId = $decoder->string();

                    $requested = [];
                    $topicCount = $decoder->int32();
                    for ($i = 0; $i < $topicCount; $i++) {
                        $topic = $decoder->string();
                        $partitionCount = $decoder->int32();
                        for ($p = 0; $p < $partitionCount; $p++) {
                            $requested[$topic][] = $decoder->int32();
                        }
                    }

                    $body = (new Encoder())->int32(count($requested));
                    foreach ($requested as $topic => $partitions) {
                        $body->string((string) $topic)->int32(count($partitions));
                        foreach ($partitions as $partition) {
                            $offset = $committed[$groupId][$topic][$partition] ?? -1;
                            $body
                                ->int32($partition)
                                ->int64($offset)
                                ->nullableString(null)
                                ->int16(0);
                        }
                    }
                    $body->int16(0);
                    break;

                default:
                    break 2;
            }
        } catch (Throwable $exception) {
            fwrite(STDERR, $exception->getMessage() . "\n");
            break;
        }

        if (!$send($connection, $correlationId, $body)) {
            break;
        }
    }

    fclose($connection);
}
