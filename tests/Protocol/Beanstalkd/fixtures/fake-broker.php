<?php

declare(strict_types=1);

// Minimal scripted Beanstalkd broker used by the protocol tests.
// Usage: php fake-broker.php <normal|timeout|malformed>
// Prints the listening port to stdout, then serves the first client.

$mode = $argv[1] ?? 'normal';

$server = stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
if ($server === false) {
    fwrite(STDERR, sprintf("listen failed: %s (%d)\n", $errorMessage, $errorCode));
    exit(1);
}

$address = stream_socket_get_name($server, false);
$separator = is_string($address) ? strrpos($address, ':') : false;
if ($separator === false) {
    fwrite(STDERR, "cannot determine listening port\n");
    exit(1);
}

fwrite(STDOUT, substr($address, $separator + 1) . "\n");
fflush(STDOUT);

$client = stream_socket_accept($server, 10.0);
if ($client === false) {
    exit(1);
}

if ($mode === 'malformed') {
    while (fgets($client) !== false) {
        fwrite($client, "WAT\r\n");
    }

    exit(0);
}

function respond($client, string $line): void
{
    fwrite($client, $line);
}

function read_bytes($client, int $length): string
{
    $data = '';
    while (strlen($data) < $length) {
        $chunk = fread($client, $length - strlen($data));
        if ($chunk === false || $chunk === '') {
            break;
        }

        $data .= $chunk;
    }

    return $data;
}

$usedTube = 'default';
$watched = ['default'];
$jobs = [];
$reserved = [];
$nextId = 0;

while (($line = fgets($client)) !== false) {
    $parts = explode(' ', rtrim($line, "\r\n"));
    $command = array_shift($parts);

    switch ($command) {
        case 'use':
            $usedTube = $parts[0] ?? '';
            respond($client, 'USING ' . $usedTube . "\r\n");
            break;

        case 'watch':
            $tube = $parts[0] ?? '';
            if (!in_array($tube, $watched, true)) {
                $watched[] = $tube;
            }
            respond($client, 'WATCHING ' . count($watched) . "\r\n");
            break;

        case 'ignore':
            $tube = $parts[0] ?? '';
            $watched = array_values(array_filter($watched, static fn (string $item): bool => $item !== $tube));
            if ($watched === []) {
                $watched = [$tube];
                respond($client, "NOT_IGNORED\r\n");
            } else {
                respond($client, 'WATCHING ' . count($watched) . "\r\n");
            }
            break;

        case 'put':
            $priority = (int) ($parts[0] ?? 0);
            $delay = (int) ($parts[1] ?? 0);
            $ttr = (int) ($parts[2] ?? 0);
            $bytes = (int) ($parts[3] ?? 0);
            $payload = read_bytes($client, $bytes + 2);
            ++$nextId;
            $jobs[$nextId] = [
                'id' => $nextId,
                'tube' => $usedTube,
                'body' => substr($payload, 0, $bytes),
                'pri' => $priority,
                'delay' => $delay,
                'ttr' => $ttr,
            ];
            respond($client, 'INSERTED ' . $nextId . "\r\n");
            break;

        case 'reserve-with-timeout':
            $timeout = (int) ($parts[0] ?? 0);
            if ($mode === 'timeout') {
                // Only the expected conversion (1500ms -> 2s) succeeds.
                respond($client, $timeout === 2 ? "TIMED_OUT\r\n" : "BAD_FORMAT\r\n");
                break;
            }

            $foundId = null;
            foreach ($jobs as $id => $job) {
                if (in_array($job['tube'], $watched, true)) {
                    $foundId = $id;
                    break;
                }
            }

            if ($foundId === null) {
                respond($client, "TIMED_OUT\r\n");
                break;
            }

            $job = $jobs[$foundId];
            unset($jobs[$foundId]);
            $reserved[$foundId] = $job;
            respond($client, sprintf(
                "RESERVED %d %d\r\n%s\r\n",
                $foundId,
                strlen($job['body']),
                $job['body'],
            ));
            break;

        case 'delete':
            $id = (int) ($parts[0] ?? 0);
            if (isset($reserved[$id])) {
                unset($reserved[$id]);
                respond($client, "DELETED\r\n");
            } else {
                respond($client, "NOT_FOUND\r\n");
            }
            break;

        case 'release':
            $id = (int) ($parts[0] ?? 0);
            $priority = (int) ($parts[1] ?? 1024);
            if (isset($reserved[$id])) {
                $job = $reserved[$id];
                unset($reserved[$id]);
                $job['pri'] = $priority;
                $jobs[$id] = $job;
                respond($client, "RELEASED\r\n");
            } else {
                respond($client, "NOT_FOUND\r\n");
            }
            break;

        case 'stats-job':
            $id = (int) ($parts[0] ?? 0);
            $job = $reserved[$id] ?? $jobs[$id] ?? null;
            if ($job === null) {
                respond($client, "NOT_FOUND\r\n");
                break;
            }
            $yaml = "---\nid: {$job['id']}\ntube: {$job['tube']}\npri: {$job['pri']}\nreserves: 1\n";
            respond($client, 'OK ' . strlen($yaml) . "\r\n" . $yaml . "\r\n");
            break;

        default:
            respond($client, "UNKNOWN_COMMAND\r\n");
    }
}

fclose($client);
fclose($server);
