<?php

declare(strict_types=1);

// Minimal Azure Service Bus REST server used by the protocol tests.
// Usage: php fake_servicebus_server.php [keyName] [key] [port]
// Prints the listening port to stdout, then serves requests until killed.

$keyName = $argv[1] ?? 'RootManageSharedAccessKey';
$key = $argv[2] ?? 'dGVzdC1rZXktZm9yLXNlcnZpY2UtYnVz';
$port = (int) ($argv[3] ?? 0);

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

/** @var array<string, list<array<string, mixed>>> $messages */
$messages = [];
$nextLock = 0;
$nextSequence = 0;

while (($client = stream_socket_accept($server, 30.0)) !== false) {
    $request = read_request($client);
    if ($request === null) {
        fclose($client);
        continue;
    }

    [$method, $target, $headers, $body] = $request;
    $questionMark = strpos($target, '?');
    $rawPath = $questionMark === false ? $target : substr($target, 0, $questionMark);
    $path = rawurldecode($rawPath);

    if (!authorized($headers['authorization'] ?? '', $keyName, $key)) {
        respond($client, [401, '{"error":"SAS authentication failed"}', [], '', 'application/json']);
        fclose($client);
        continue;
    }

    if (str_contains($path, 'missing')) {
        respond($client, [410, '{"error":"entity does not exist"}', [], '', 'application/json']);
        fclose($client);
        continue;
    }

    if (str_contains($path, 'boom')) {
        respond($client, [500, '{"error":"server exploded"}', [], '', 'application/json']);
        fclose($client);
        continue;
    }

    if ($method === 'POST' && preg_match('~^/(.+)/messages$~', $path, $matches) === 1) {
        respond($client, send_message($matches[1], $headers, $body, $messages, $nextSequence));
    } elseif ($method === 'POST' && preg_match('~^/(.+)/messages/head$~', $path, $matches) === 1) {
        respond($client, receive_message($matches[1], $headers, $messages, $nextLock));
    } elseif (($method === 'DELETE' || $method === 'PUT')
        && preg_match('~^/(.+)/messages/([^/]+)/([^/]+)$~', $path, $matches) === 1
    ) {
        respond($client, settle_message(
            $matches[1],
            rawurldecode($matches[2]),
            rawurldecode($matches[3]),
            $method,
            $messages,
        ));
    } else {
        respond($client, [404, '{"error":"not found"}', [], '', 'application/json']);
    }

    fclose($client);
}

/**
 * @param array<string, string> $headers
 * @param array<string, list<array<string, mixed>>> $messages
 * @return array{0: int, 1: string, 2: array<string, string>, 3: string, 4: string}
 */
function send_message(string $entity, array $headers, string $body, array &$messages, int &$sequence): array
{
    $properties = json_decode($headers['brokerproperties'] ?? '', true);
    if (!is_array($properties)) {
        return [400, '{"error":"BrokerProperties must be a JSON object"}', [], '', 'application/json'];
    }

    $id = $properties['MessageId'] ?? null;
    if (!is_string($id) || $id === '') {
        return [400, '{"error":"MessageId is required"}', [], '', 'application/json'];
    }

    ++$sequence;
    $scheduled = $properties['ScheduledEnqueueTimeUtc'] ?? null;

    $messages[$entity][] = [
        'id' => $id,
        'properties' => $properties,
        'body' => $body,
        'content_type' => $headers['content-type'] ?? 'application/octet-stream',
        'custom' => custom_headers($headers),
        'visible_at' => is_string($scheduled) ? (int) strtotime($scheduled) : 0,
        'sequence' => $sequence,
        'delivery' => 0,
        'lock' => null,
    ];

    return [201, '', [], 'application/xml; charset=utf-8', 'application/xml; charset=utf-8'];
}

/**
 * @param array<string, string> $headers
 * @param array<string, list<array<string, mixed>>> $messages
 * @return array{0: int, 1: string, 2: array<string, string>, 3: string, 4: string}
 */
function receive_message(string $entity, array $headers, array &$messages, int &$nextLock): array
{
    $now = time();

    foreach ($messages[$entity] ?? [] as $index => $message) {
        if ($message['lock'] !== null || $message['visible_at'] > $now) {
            continue;
        }

        $lock = sprintf('lock-%d', ++$nextLock);
        $delivery = $message['delivery'] + 1;
        $messages[$entity][$index]['lock'] = $lock;
        $messages[$entity][$index]['delivery'] = $delivery;

        $properties = $message['properties'];
        $properties['LockToken'] = $lock;
        $properties['DeliveryCount'] = $delivery;
        $properties['SequenceNumber'] = $message['sequence'];
        $properties['LockedUntilUtc'] = gmdate('D, d M Y H:i:s \G\M\T', $now + 60);

        $host = $headers['host'] ?? '127.0.0.1';
        $location = 'http://' . $host . '/' . $entity . '/messages/'
            . rawurlencode($message['id']) . '/' . rawurlencode($lock);

        $responseHeaders = [
            'BrokerProperties' => (string) json_encode($properties, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES),
            'Location' => $location,
            'Content-Type' => (string) $message['content_type'],
        ];

        foreach ($message['custom'] as $name => $value) {
            $responseHeaders[$name] = $value;
        }

        return [201, (string) $message['body'], $responseHeaders, '', (string) $message['content_type']];
    }

    return [204, '', [], '', 'application/xml; charset=utf-8'];
}

/**
 * @param array<string, list<array<string, mixed>>> $messages
 * @return array{0: int, 1: string, 2: array<string, string>, 3: string, 4: string}
 */
function settle_message(string $entity, string $id, string $token, string $method, array &$messages): array
{
    foreach ($messages[$entity] ?? [] as $index => $message) {
        if ($message['id'] !== $id || $message['lock'] !== $token) {
            continue;
        }

        if ($method === 'DELETE') {
            unset($messages[$entity][$index]);
            $messages[$entity] = array_values($messages[$entity]);
        } else {
            $messages[$entity][$index]['lock'] = null;
        }

        return [200, '', [], '', 'application/xml; charset=utf-8'];
    }

    return [404, '{"error":"No message was found for the specified lock token"}', [], '', 'application/json'];
}

/**
 * @param array<string, string> $headers
 * @return array<string, string>
 */
function custom_headers(array $headers): array
{
    $reserved = [
        'authorization',
        'brokerproperties',
        'contenttype',
        'contentlength',
        'host',
        'useragent',
        'accept',
        'acceptencoding',
        'connection',
        'expect',
    ];

    $custom = [];
    foreach ($headers as $name => $value) {
        $normalized = strtolower((string) preg_replace('/[^a-z0-9]/', '', $name));
        if (in_array($normalized, $reserved, true) || str_starts_with($normalized, 'xms')) {
            continue;
        }

        $custom[$name] = $value;
    }

    return $custom;
}

function authorized(string $header, string $keyName, string $key): bool
{
    $prefix = 'SharedAccessSignature ';
    if (!str_starts_with($header, $prefix)) {
        return false;
    }

    $parameters = [];
    foreach (explode('&', substr($header, strlen($prefix))) as $part) {
        $pair = explode('=', $part, 2);
        if (count($pair) === 2) {
            $parameters[$pair[0]] = $pair[1];
        }
    }

    $resource = $parameters['sr'] ?? '';
    $signature = urldecode($parameters['sig'] ?? '');
    $expires = (int) ($parameters['se'] ?? 0);
    $actualKeyName = urldecode($parameters['skn'] ?? '');

    if ($resource === '' || $actualKeyName !== $keyName || $expires <= time()) {
        return false;
    }

    $expected = base64_encode(hash_hmac('sha256', $resource . "\n" . $expires, $key, true));

    return hash_equals($expected, $signature);
}

/**
 * @param resource $client
 * @return array{0: string, 1: string, 2: array<string, string>, 3: string}|null
 */
function read_request($client): ?array
{
    $line = fgets($client);
    if ($line === false) {
        return null;
    }

    $parts = explode(' ', trim($line));
    $method = $parts[0] ?? 'GET';
    $target = $parts[1] ?? '/';

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

    if (strtolower($headers['expect'] ?? '') === '100-continue') {
        fwrite($client, "HTTP/1.1 100 Continue\r\n\r\n");
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

    return [$method, $target, $headers, $body];
}

/**
 * @param resource $client
 * @param array{0: int, 1: string, 2: array<string, string>, 3: string, 4: string} $result
 */
function respond($client, array $result): void
{
    [$status, $body, $headers, , $contentType] = $result;

    $reasons = [
        200 => 'OK',
        201 => 'Created',
        204 => 'No Content',
        400 => 'Bad Request',
        401 => 'Unauthorized',
        404 => 'Not Found',
        405 => 'Method Not Allowed',
        410 => 'Gone',
        500 => 'Internal Server Error',
    ];

    $headers = array_merge([
        'Content-Type' => $contentType === '' ? 'application/json' : $contentType,
        'Content-Length' => (string) strlen($body),
        'Connection' => 'close',
    ], $headers);

    $head = sprintf("HTTP/1.1 %d %s\r\n", $status, $reasons[$status] ?? 'OK');
    foreach ($headers as $name => $value) {
        $head .= $name . ': ' . $value . "\r\n";
    }

    fwrite($client, $head . "\r\n" . $body);
}
