<?php

declare(strict_types=1);

/**
 * Minimal scripted RESP2 server for Redis Streams driver protocol tests.
 *
 * Usage: php fake_redis_server.php <scenario> <command-log-file>
 * Scenarios: echo (default), busygroup, malformed, auth-failure.
 */

$scenario = $argv[1] ?? 'echo';
$logFile = $argv[2] ?? null;

$server = stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
if ($server === false) {
    fwrite(STDERR, sprintf("listen failed: %s (%d)\n", $errorMessage, $errorCode));
    exit(1);
}

$address = (string) stream_socket_get_name($server, false);
$separator = (int) strrpos($address, ':');
$port = substr($address, $separator + 1);
fwrite(STDOUT, $port . "\n");
fflush(STDOUT);

$peer = @stream_socket_accept($server, 10.0);
if ($peer === false) {
    exit(0);
}

stream_set_timeout($peer, 10);

/**
 * @param resource $peer
 * @return list<string>|null
 */
function readCommand($peer): ?array
{
    $line = fgets($peer);
    if ($line === false || $line === '' || $line[0] !== '*') {
        return null;
    }

    $count = (int) substr(rtrim($line, "\r\n"), 1);
    $arguments = [];

    for ($i = 0; $i < $count; $i++) {
        $header = fgets($peer);
        if ($header === false || $header === '' || $header[0] !== '$') {
            return null;
        }

        $length = (int) substr(rtrim($header, "\r\n"), 1);
        $data = '';
        while (strlen($data) < $length) {
            $chunk = fread($peer, $length - strlen($data));
            if ($chunk === false || $chunk === '') {
                return null;
            }

            $data .= $chunk;
        }

        fread($peer, 2);
        $arguments[] = $data;
    }

    return $arguments;
}

function encodeReply(mixed $value): string
{
    if ($value === null) {
        return "$-1\r\n";
    }

    if (is_int($value)) {
        return ':' . $value . "\r\n";
    }

    if (is_array($value)) {
        $encoded = '*' . count($value) . "\r\n";
        foreach ($value as $item) {
            $encoded .= encodeReply($item);
        }

        return $encoded;
    }

    $string = (string) $value;

    return '$' . strlen($string) . "\r\n" . $string . "\r\n";
}

$pending = [];
$sequence = 0;
$reads = 0;

while (($command = readCommand($peer)) !== null) {
    if ($logFile !== null) {
        file_put_contents(
            $logFile,
            json_encode($command, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE) . "\n",
            FILE_APPEND,
        );
    }

    $name = strtoupper($command[0] ?? '');

    switch ($name) {
        case 'AUTH':
            if ($scenario === 'auth-failure') {
                fwrite($peer, "-WRONGPASS invalid username-password pair or user is disabled.\r\n");
                break;
            }

            fwrite($peer, "+OK\r\n");
            break;

        case 'SELECT':
            fwrite($peer, "+OK\r\n");
            break;

        case 'XGROUP':
            if ($scenario === 'busygroup') {
                fwrite($peer, "-BUSYGROUP Consumer Group name already exists\r\n");
                break;
            }

            fwrite($peer, "+OK\r\n");
            break;

        case 'XADD':
            $star = array_search('*', $command, true);
            $fields = $star === false ? [] : array_slice($command, $star + 1);
            if ($fields === [] || count($fields) % 2 !== 0) {
                fwrite($peer, "-ERR wrong number of arguments for 'xadd' command\r\n");
                break;
            }

            $id = sprintf('1700000000000-%d', $sequence++);
            $pending[] = ['id' => $id, 'fields' => $fields];
            fwrite($peer, encodeReply($id));
            break;

        case 'XREADGROUP':
            $reads++;
            if ($scenario === 'malformed') {
                fwrite($peer, "!BROKEN\r\n");
                break;
            }

            if ($scenario === 'nogroup' && $reads === 1) {
                fwrite($peer, "-NOGROUP No such key 'orders' or consumer group 'workers'\r\n");
                break;
            }

            $streams = array_search('STREAMS', $command, true);
            $stream = $streams === false ? 'unknown' : ($command[$streams + 1] ?? 'unknown');
            $entry = array_shift($pending);
            if ($entry === null) {
                fwrite($peer, "*-1\r\n");
                break;
            }

            fwrite($peer, encodeReply([[$stream, [[$entry['id'], $entry['fields']]]]]));
            break;

        case 'XACK':
            fwrite($peer, ":1\r\n");
            break;

        case 'DEL':
            fwrite($peer, ":1\r\n");
            break;

        default:
            fwrite($peer, "-ERR unknown command\r\n");
    }
}

fclose($peer);
fclose($server);
