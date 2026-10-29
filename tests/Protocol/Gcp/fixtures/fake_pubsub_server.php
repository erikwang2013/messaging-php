<?php

declare(strict_types=1);

// Minimal scripted Pub/Sub REST server used by the protocol tests.
// Usage: php fake_pubsub_server.php <normal|error|malformed> [port]
// Prints the listening port to stdout, then serves one request per connection.

$mode = $argv[1] ?? 'normal';
$port = (int) ($argv[2] ?? 0);

$server = stream_socket_server('tcp://127.0.0.1:' . $port, $errorCode, $errorMessage);
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

$messages = [];
$pending = [];
$requests = [];
$nextMessageId = 0;
$nextAckId = 0;

while (($client = stream_socket_accept($server, 30.0)) !== false) {
    $request = read_request($client);
    if ($request === null) {
        fclose($client);
        continue;
    }
    [$method, $path, $body] = $request;

    if ($method === 'GET' && $path === '/__state') {
        // Inspection endpoint for protocol assertions; not part of the Pub/Sub API.
        respond($client, 200, json_encode(['requests' => $requests], JSON_THROW_ON_ERROR));
        fclose($client);
        continue;
    }

    $requests[] = ['method' => $method, 'path' => $path, 'body' => json_decode($body, true)];

    if ($mode === 'error') {
        respond($client, 500, '{"error":{"code":500,"message":"emulator exploded","status":"INTERNAL"}}');
        fclose($client);
        continue;
    }

    if ($mode === 'malformed') {
        respond($client, 200, 'not-json', 'text/plain');
        fclose($client);
        continue;
    }

    if ($method === 'PUT' && (str_contains($path, '/topics/') || str_contains($path, '/subscriptions/'))) {
        // Topic/subscription provisioning used by the integration tests.
        respond($client, 200, '{}');
    } elseif ($method === 'DELETE' && (str_contains($path, '/topics/') || str_contains($path, '/subscriptions/'))) {
        respond($client, 200, '{}');
    } elseif (str_ends_with($path, ':publish')) {
        $payload = json_decode($body, true);
        $published = is_array($payload) && is_array($payload['messages'] ?? null) ? $payload['messages'] : [];
        $ids = [];
        foreach ($published as $entry) {
            $entry = is_array($entry) ? $entry : [];
            ++$nextMessageId;
            $messageId = 'm-' . $nextMessageId;
            $messages[] = [
                'data' => is_string($entry['data'] ?? null) ? $entry['data'] : '',
                'attributes' => is_array($entry['attributes'] ?? null) ? $entry['attributes'] : [],
                'orderingKey' => is_string($entry['orderingKey'] ?? null) ? $entry['orderingKey'] : null,
                'messageId' => $messageId,
            ];
            $ids[] = $messageId;
        }
        respond($client, 200, json_encode(['messageIds' => $ids], JSON_THROW_ON_ERROR));
    } elseif (str_ends_with($path, ':pull')) {
        if ($messages === []) {
            respond($client, 200, '{}');
        } else {
            $message = array_shift($messages);
            ++$nextAckId;
            $ackId = 'a-' . $nextAckId;
            $pending[$ackId] = $message;
            respond($client, 200, json_encode([
                'receivedMessages' => [['ackId' => $ackId, 'message' => $message]],
            ], JSON_THROW_ON_ERROR));
        }
    } elseif (str_ends_with($path, ':acknowledge')) {
        $payload = json_decode($body, true);
        $ackIds = is_array($payload) && is_array($payload['ackIds'] ?? null) ? $payload['ackIds'] : [];
        foreach ($ackIds as $ackId) {
            unset($pending[$ackId]);
        }
        respond($client, 200, '{}');
    } elseif (str_ends_with($path, ':modifyAckDeadline')) {
        $payload = json_decode($body, true);
        $ackIds = is_array($payload) && is_array($payload['ackIds'] ?? null) ? $payload['ackIds'] : [];
        $seconds = is_array($payload) ? (int) ($payload['ackDeadlineSeconds'] ?? 0) : 0;
        foreach ($ackIds as $ackId) {
            if ($seconds === 0 && isset($pending[$ackId])) {
                array_unshift($messages, $pending[$ackId]);
                unset($pending[$ackId]);
            }
        }
        respond($client, 200, '{}');
    } else {
        respond($client, 404, '{"error":{"code":404,"message":"not found"}}');
    }

    fclose($client);
}

/**
 * @param resource $client
 * @return array{0: string, 1: string, 2: string}|null
 */
function read_request($client): ?array
{
    $line = fgets($client);
    if ($line === false) {
        return null;
    }

    $parts = explode(' ', trim($line));
    $method = $parts[0] ?? 'GET';
    $path = $parts[1] ?? '/';

    $headers = [];
    while (($line = fgets($client)) !== false) {
        $line = rtrim($line, "\r\n");
        if ($line === '') {
            break;
        }
        $pair = explode(':', $line, 2);
        if (count($pair) === 2) {
            $headers[strtolower(trim($pair[0]))] = trim($pair[1]);
        }
    }

    $length = (int) ($headers['content-length'] ?? 0);
    $body = '';
    while (strlen($body) < $length) {
        $chunk = fread($client, $length - strlen($body));
        if ($chunk === false || $chunk === '') {
            break;
        }
        $body .= $chunk;
    }

    return [$method, $path, $body];
}

/**
 * @param resource $client
 */
function respond($client, int $status, string $body, string $type = 'application/json'): void
{
    $reasons = [200 => 'OK', 400 => 'Bad Request', 404 => 'Not Found', 500 => 'Internal Server Error'];
    fwrite($client, sprintf(
        "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
        $status,
        $reasons[$status] ?? 'OK',
        $type,
        strlen($body),
        $body,
    ));
}
