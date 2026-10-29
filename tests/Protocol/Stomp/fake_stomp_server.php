<?php

declare(strict_types=1);

/*
 * Minimal scripted STOMP 1.2 server used by StompProtocolTest.
 *
 * Usage: php fake_stomp_server.php <mode> <log-file>
 * Modes: echo, silent, connect-error, subscribe-error, send-error.
 *
 * Prints the listening port on stdout, accepts a single client and logs every
 * received frame as a base64 line to <log-file> so tests can assert wire output.
 */

$mode = $argv[1] ?? 'echo';
$logFile = $argv[2] ?? '';

$server = @stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
if ($server === false) {
    fwrite(STDERR, sprintf("fake STOMP server failed to listen: %s (%d)\n", $errorMessage, $errorCode));
    exit(1);
}

$address = stream_socket_get_name($server, false);
if (!is_string($address)) {
    exit(1);
}
$separator = strrpos($address, ':');
$port = (int) substr($address, (int) $separator + 1);
fwrite(STDOUT, $port . "\n");
fflush(STDOUT);

$client = @stream_socket_accept($server, 15);
if ($client === false) {
    exit(1);
}
stream_set_timeout($client, 15);

$buffer = '';

$fill = static function () use ($client, &$buffer): bool {
    $chunk = @fread($client, 65536);
    if ($chunk === false || $chunk === '') {
        return false;
    }

    $buffer .= $chunk;

    return true;
};

$nextFrame = static function () use (&$buffer, $fill): ?string {
    while (true) {
        $buffer = ltrim($buffer, "\r\n");

        $lf = strpos($buffer, "\n\n");
        $crlf = strpos($buffer, "\r\n\r\n");
        if ($lf !== false && ($crlf === false || $lf <= $crlf)) {
            $position = $lf;
            $length = 2;
        } elseif ($crlf !== false) {
            $position = $crlf;
            $length = 4;
        } else {
            if (!$fill()) {
                return null;
            }
            continue;
        }

        $headerBlock = substr($buffer, 0, $position);
        $bodyStart = $position + $length;
        $contentLength = preg_match('/(?:^|\r?\n)content-length:\s*(\d+)/i', $headerBlock, $matches) === 1
            ? (int) $matches[1]
            : null;

        if ($contentLength === null) {
            $end = strpos($buffer, "\x00", $bodyStart);
            if ($end === false) {
                if (!$fill()) {
                    return null;
                }
                continue;
            }
        } else {
            $end = $bodyStart + $contentLength;
            if (strlen($buffer) <= $end) {
                if (!$fill()) {
                    return null;
                }
                continue;
            }
        }

        $raw = substr($buffer, 0, $end + 1);
        $buffer = substr($buffer, $end + 1);

        return $raw;
    }
};

$log = static function (string $raw) use ($logFile): void {
    if ($logFile === '') {
        return;
    }

    file_put_contents($logFile, base64_encode($raw) . "\n", FILE_APPEND | LOCK_EX);
};

$unescape = static function (string $value): string {
    if (!str_contains($value, '\\')) {
        return $value;
    }

    $result = '';
    $length = strlen($value);
    for ($i = 0; $i < $length; $i++) {
        if ($value[$i] !== '\\') {
            $result .= $value[$i];
            continue;
        }

        $i++;
        $result .= match ($value[$i] ?? '') {
            '\\' => '\\',
            'n' => "\n",
            'r' => "\r",
            'c' => ':',
            default => '',
        };
    }

    return $result;
};

$escape = static fn (string $value): string => strtr($value, [
    '\\' => '\\\\',
    "\r" => '\\r',
    "\n" => '\\n',
    ':' => '\\c',
]);

/**
 * @return array{string, array<string, string>, string}
 */
$parse = static function (string $raw) use ($unescape): array {
    $raw = substr($raw, 0, -1);

    $lf = strpos($raw, "\n\n");
    $crlf = strpos($raw, "\r\n\r\n");
    if ($lf !== false && ($crlf === false || $lf <= $crlf)) {
        $position = $lf;
        $length = 2;
    } else {
        $position = (int) $crlf;
        $length = 4;
    }

    $headerBlock = substr($raw, 0, $position);
    $body = substr($raw, $position + $length);

    $lines = explode("\n", $headerBlock);
    $command = rtrim((string) array_shift($lines), "\r");
    $escapeHeaders = !in_array($command, ['CONNECT', 'STOMP', 'CONNECTED'], true);

    $headers = [];
    foreach ($lines as $line) {
        $line = rtrim($line, "\r");
        $colon = strpos($line, ':');
        if ($colon === false) {
            continue;
        }

        $value = substr($line, $colon + 1);
        $headers[substr($line, 0, $colon)] = $escapeHeaders ? $unescape($value) : $value;
    }

    return [$command, $headers, $body];
};

$send = static function (string $payload) use ($client): void {
    @fwrite($client, $payload);
};

$handshake = $nextFrame();
if ($handshake === null) {
    exit(0);
}

$log($handshake);
[$handshakeCommand] = $parse($handshake);

if ($handshakeCommand !== 'STOMP' && $handshakeCommand !== 'CONNECT') {
    $send("ERROR\nmessage:expected STOMP frame\n\n\x00");
    exit(0);
}

if ($mode === 'connect-error') {
    $send("ERROR\nmessage:Unable to validate user: bad credentials\n\n\x00");
    exit(0);
}

$send("CONNECTED\nversion:1.2\nsession:fake-session\nheart-beat:0,0\n\n\x00");

$subscriptions = [];
$counter = 0;

while (($raw = $nextFrame()) !== null) {
    $log($raw);
    [$command, $headers, $body] = $parse($raw);

    switch ($command) {
        case 'SUBSCRIBE':
            $subscriptions[$headers['destination'] ?? ''] = $headers['id'] ?? '';
            if ($mode === 'subscribe-error') {
                $send("ERROR\nmessage:subscription rejected\n\n\x00");
                exit(0);
            }
            break;

        case 'SEND':
            if ($mode === 'send-error') {
                $send("ERROR\nmessage:send rejected\n\n\x00");
                break;
            }

            if ($mode !== 'echo') {
                break;
            }

            $counter++;
            $destination = $headers['destination'] ?? '';
            $subscription = $subscriptions[$destination] ?? 'unknown';

            $message = "MESSAGE\n";
            $message .= 'destination:' . $escape($destination) . "\n";
            $message .= 'subscription:' . $escape($subscription) . "\n";
            $message .= 'message-id:srv-' . $counter . "\n";
            $message .= 'ack:ack-' . $counter . "\n";
            $message .= 'content-length:' . strlen($body) . "\n";

            foreach ($headers as $name => $value) {
                $normalized = strtolower($name);
                if (in_array($normalized, ['destination', 'content-length', 'receipt'], true)) {
                    continue;
                }

                $message .= $name . ':' . $escape($value) . "\n";
            }

            $send($message . "\n" . $body . "\x00");
            break;

        case 'ACK':
        case 'NACK':
            break;

        case 'DISCONNECT':
            exit(0);
    }
}

exit(0);
