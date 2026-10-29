<?php

declare(strict_types=1);

/**
 * Minimal scripted NATS server for protocol tests.
 *
 * Usage: php fake_nats_server.php [normal|heartbeat|error|protocol-error|silent|no-headers|reconnect|burst]
 * Prints "PORT <port>" on stdout once listening.
 */

$mode = $argv[1] ?? 'normal';

$server = @stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
if ($server === false) {
    fwrite(STDERR, sprintf("Unable to listen: %s (%d)\n", $errorMessage, $errorCode));
    exit(1);
}

$address = stream_socket_get_name($server, false);
if (!is_string($address)) {
    exit(1);
}
$port = (int) substr($address, (int) strrpos($address, ':') + 1);
fwrite(STDOUT, "PORT {$port}\n");
fflush(STDOUT);

$info = 'INFO ' . json_encode([
    'server_id' => 'fake',
    'version' => '2.10.0',
    'proto' => 1,
    'headers' => $mode !== 'no-headers',
]) . "\r\n";

if ($mode === 'reconnect') {
    // Complete the handshake, then drop the idle connection. The driver is
    // expected to reconnect and retry the publish on the second connection.
    $first = @stream_socket_accept($server, 10.0);
    if ($first === false) {
        exit(1);
    }
    stream_set_timeout($first, 30);
    fwrite($first, $info);
    completeHandshake($first);
    fclose($first);
}

$client = @stream_socket_accept($server, 10.0);
if ($client === false) {
    exit(1);
}
stream_set_timeout($client, 30);

fwrite($client, $info);

if ($mode === 'heartbeat') {
    fwrite($client, "PING\r\n");
}

if ($mode === 'silent') {
    // Read the client's handshake but never answer, so the driver times out.
    while (true) {
        $chunk = @fread($client, 65536);
        if ($chunk === false || ($chunk === '' && feof($client))) {
            exit(0);
        }
    }
}

/** @var array<string, string> $subscriptions sid => subject */
$subscriptions = [];
$buffer = '';
$pending = null;
$heartbeatSeen = false;

while (!feof($client)) {
    $chunk = @fread($client, 65536);
    if ($chunk === false) {
        break;
    }
    if ($chunk === '') {
        if (feof($client)) {
            break;
        }

        continue;
    }

    $buffer .= $chunk;

    while (true) {
        if ($pending !== null) {
            $needed = $pending['total'] + 2;
            if (strlen($buffer) < $needed) {
                break;
            }

            $payload = substr($buffer, 0, $pending['total']);
            $buffer = substr($buffer, $needed);
            deliver($client, $subscriptions, $pending, $payload);
            $pending = null;

            continue;
        }

        $position = strpos($buffer, "\r\n");
        if ($position === false) {
            break;
        }

        $line = substr($buffer, 0, $position);
        $buffer = substr($buffer, $position + 2);

        if (str_starts_with($line, 'CONNECT')) {
            if ($mode === 'error') {
                fwrite($client, "-ERR 'Authorization Violation'\r\n");
                exit(0);
            }
            if ($mode === 'protocol-error') {
                fwrite($client, "-ERR 'Unknown Protocol Operation'\r\n");
                exit(0);
            }

            continue;
        }

        if ($line === 'PING') {
            fwrite($client, "PONG\r\n");

            continue;
        }

        if ($line === 'PONG') {
            $heartbeatSeen = true;

            continue;
        }

        if (str_starts_with($line, 'SUB ')) {
            $parts = preg_split('/ +/', $line) ?: [];
            $subject = $parts[1] ?? '';
            $sid = $parts[count($parts) - 1] ?? '';
            $subscriptions[$sid] = $subject;

            if ($mode === 'heartbeat' && $heartbeatSeen) {
                $body = 'heartbeat-ok';
                fwrite($client, sprintf("MSG %s %s %d\r\n%s\r\n", $subject, $sid, strlen($body), $body));
            }

            if ($mode === 'burst') {
                for ($number = 1; $number <= 3; $number++) {
                    $body = 'burst-' . $number;
                    fwrite($client, sprintf("MSG %s %s %d\r\n%s\r\n", $subject, $sid, strlen($body), $body));
                }
            }

            continue;
        }

        if (str_starts_with($line, 'PUB ') || str_starts_with($line, 'HPUB ')) {
            $parts = preg_split('/ +/', $line) ?: [];
            $withHeaders = $parts[0] === 'HPUB';
            $total = (int) $parts[count($parts) - 1];
            $headerSize = $withHeaders ? (int) $parts[count($parts) - 2] : null;
            $replyIndex = count($parts) - ($withHeaders ? 3 : 2);
            $reply = $replyIndex >= 3 ? $parts[$replyIndex] : null;
            $pending = [
                'subject' => $parts[1],
                'reply' => $reply,
                'total' => $total,
                'header' => $headerSize,
            ];

            continue;
        }

        // Ignore anything else (PING/PONG/+OK handling above covers the client).
    }
}

exit(0);

/**
 * Reads CONNECT/PING from the client and answers PONG to the handshake PING.
 *
 * @param resource $client
 */
function completeHandshake($client): void
{
    $buffer = '';
    while (!feof($client)) {
        $chunk = @fread($client, 65536);
        if ($chunk === false || $chunk === '') {
            return;
        }

        $buffer .= $chunk;
        while (($position = strpos($buffer, "\r\n")) !== false) {
            $line = substr($buffer, 0, $position);
            $buffer = substr($buffer, $position + 2);

            if ($line === 'PING') {
                fwrite($client, "PONG\r\n");

                return;
            }
        }
    }
}

/**
 * @param resource $client
 * @param array<string, string> $subscriptions
 * @param array{subject: string, reply: string|null, total: int, header: int|null} $pending
 */
function deliver($client, array $subscriptions, array $pending, string $payload): void
{
    foreach ($subscriptions as $sid => $subject) {
        if ($subject !== $pending['subject']) {
            continue;
        }

        $reply = $pending['reply'] === null ? '' : ' ' . $pending['reply'];
        if ($pending['header'] !== null) {
            fwrite($client, sprintf(
                "HMSG %s %s%s %d %d\r\n%s\r\n",
                $pending['subject'],
                $sid,
                $reply,
                $pending['header'],
                strlen($payload),
                $payload,
            ));
        } else {
            fwrite($client, sprintf(
                "MSG %s %s%s %d\r\n%s\r\n",
                $pending['subject'],
                $sid,
                $reply,
                strlen($payload),
                $payload,
            ));
        }
    }
}
