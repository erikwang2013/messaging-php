<?php

declare(strict_types=1);

/**
 * Minimal scripted ZMTP 3.1 (NULL mechanism) peer for protocol tests.
 *
 * Usage: php fake_zmtp_peer.php <mode>
 *
 * Modes:
 *   relay         two connections relayed to each other (PUB->SUB, PUSH->PULL)
 *   multipart     PUB peer sends one multipart message after a subscription
 *   split-frame   sends a frame header, pauses, then the size and body
 *   bad-greeting  answers the greeting with 64 zero bytes
 *   bad-frame     sends a frame with reserved flags after the subscription
 *   wrong-type    completes READY with a peer socket type that cannot connect
 *   silent        accepts the connection and never answers
 *   no-ready      sends a data frame where the READY command is expected
 *
 * Prints "PORT <port>" on stdout once listening.
 */

$mode = $argv[1] ?? 'relay';

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

if ($mode === 'bad-greeting') {
    $client = acceptClient($server);
    writeAll($client, str_repeat("\x00", 64));
    hold($client);
}

if ($mode === 'silent') {
    $client = acceptClient($server);
    while (!feof($client)) {
        $chunk = @fread($client, 65536);
        if ($chunk === false) {
            break;
        }
        if ($chunk === '') {
            usleep(10_000);
        }
    }
    exit(0);
}

$first = acceptClient($server);
$firstType = handshake($first, $mode);

if ($mode === 'bad-frame') {
    readFrameBlocking($first);
    writeAll($first, chr(0x08) . chr(1) . 'x');
    hold($first);
}

if ($mode === 'multipart') {
    $subscription = readFrameBlocking($first);
    $topic = substr($subscription['body'], 1);
    writeAll($first, zmtpFrame(0x01, $topic . 'hel') . zmtpFrame(0x00, 'lo'));
    hold($first);
}

if ($mode === 'split-frame') {
    $subscription = readFrameBlocking($first);
    $body = substr($subscription['body'], 1) . 'hello';
    // Flags byte first; the reader times out before the size and body arrive.
    writeAll($first, chr(0x00));
    usleep(50_000);
    writeAll($first, chr(strlen($body)) . $body);
    hold($first);
}

$clients = [[
    'stream' => $first,
    'type' => $firstType,
    'buffer' => '',
    'parts' => [],
    'subscriptions' => [],
]];
$deadline = time() + 30;

while (time() < $deadline) {
    $read = [$server];
    foreach ($clients as $client) {
        $read[] = $client['stream'];
    }
    $write = null;
    $except = null;
    $ready = @stream_select($read, $write, $except, 1);
    if ($ready === false) {
        break;
    }
    if ($ready === 0) {
        continue;
    }

    foreach ($read as $stream) {
        if ($stream === $server) {
            $connection = @stream_socket_accept($server, 0);
            if ($connection === false) {
                continue;
            }
            stream_set_timeout($connection, 10);
            $type = handshake($connection, $mode);
            $clients[] = [
                'stream' => $connection,
                'type' => $type,
                'buffer' => '',
                'parts' => [],
                'subscriptions' => [],
            ];
            continue;
        }

        $index = null;
        foreach ($clients as $candidate => $client) {
            if ($client['stream'] === $stream) {
                $index = $candidate;
                break;
            }
        }
        if ($index === null) {
            continue;
        }

        $chunk = @fread($stream, 65536);
        if ($chunk === false || ($chunk === '' && feof($stream))) {
            fclose($stream);
            unset($clients[$index]);
            $clients = array_values($clients);
            continue;
        }
        if ($chunk === '') {
            continue;
        }

        $clients[$index]['buffer'] .= $chunk;
        while (($frame = takeFrame($clients[$index]['buffer'])) !== null) {
            handleClientFrame($clients, $index, $frame);
        }
    }
}

exit(0);

/**
 * @param resource $server
 * @return resource
 */
function acceptClient($server)
{
    $client = @stream_socket_accept($server, 10.0);
    if ($client === false) {
        exit(1);
    }
    stream_set_timeout($client, 10);

    return $client;
}

/**
 * @param resource $client
 */
function handshake($client, string $mode): string
{
    $greeting = readExactly($client, 64);
    $major = ord($greeting[10]);
    $mechanism = rtrim(substr($greeting, 12, 20), "\x00");
    if ($greeting[0] !== "\xFF" || $greeting[9] !== "\x7F" || $major !== 3 || $mechanism !== 'NULL') {
        fwrite(STDERR, "Client sent an unsupported ZMTP greeting.\n");
        exit(1);
    }

    writeAll($client, zmtpGreeting());

    if ($mode === 'no-ready') {
        writeAll($client, zmtpFrame(0x00, 'not-ready'));
        hold($client);
        exit(0);
    }

    $frame = readFrameBlocking($client);
    if (($frame['flags'] & 0x04) === 0) {
        fwrite(STDERR, "Expected a READY command frame.\n");
        exit(1);
    }

    $body = $frame['body'];
    $nameLength = $body === '' ? 0 : ord($body[0]);
    if ($nameLength === 0 || substr($body, 1, $nameLength) !== 'READY') {
        fwrite(STDERR, "Expected a READY command.\n");
        exit(1);
    }

    $properties = parseMetadata(substr($body, 1 + $nameLength));
    $clientType = $properties['Socket-Type'] ?? '';
    $peerType = compatiblePeerType($clientType);
    if ($mode === 'wrong-type') {
        $peerType = $clientType === 'SUB' ? 'PUSH' : 'PUB';
    }

    writeAll($client, readyFrame($peerType));

    return $clientType;
}

/**
 * @param resource $client
 */
function hold($client): void
{
    $deadline = time() + 10;
    while (time() < $deadline && !feof($client)) {
        $chunk = @fread($client, 65536);
        if ($chunk === false) {
            break;
        }
        if ($chunk === '') {
            usleep(10_000);
        }
    }
    exit(0);
}

/**
 * @param resource $stream
 */
function readExactly($stream, int $length): string
{
    $buffer = '';
    while (strlen($buffer) < $length) {
        $chunk = @fread($stream, $length - strlen($buffer));
        if ($chunk === false) {
            throw new RuntimeException('Read failed.');
        }
        if ($chunk === '') {
            $metadata = stream_get_meta_data($stream);
            if (feof($stream) || $metadata['timed_out']) {
                throw new RuntimeException('Peer closed or timed out.');
            }
            usleep(1000);
        }
        $buffer .= $chunk;
    }

    return $buffer;
}

/**
 * @param resource $stream
 * @return array{flags: int, body: string}
 */
function readFrameBlocking($stream): array
{
    $flags = ord(readExactly($stream, 1));
    if (($flags & 0x02) !== 0) {
        $unpacked = unpack('J', readExactly($stream, 8));
        $size = is_array($unpacked) ? (int) $unpacked[1] : -1;
    } else {
        $size = ord(readExactly($stream, 1));
    }
    if ($size < 0) {
        throw new RuntimeException('Invalid frame size.');
    }

    return ['flags' => $flags, 'body' => $size === 0 ? '' : readExactly($stream, $size)];
}

/**
 * @param resource $stream
 */
function writeAll($stream, string $data): void
{
    $length = strlen($data);
    $written = 0;
    while ($written < $length) {
        $chunk = @fwrite($stream, substr($data, $written));
        if ($chunk === false || $chunk === 0) {
            return;
        }
        $written += $chunk;
    }
}

/**
 * @param array<int, array{stream: resource, type: string, buffer: string, parts: list<string>, subscriptions: array<string, true>}> $clients
 * @param array{flags: int, body: string} $frame
 */
function handleClientFrame(array &$clients, int $index, array $frame): void
{
    if (($frame['flags'] & 0x04) !== 0) {
        return;
    }

    if (($frame['flags'] & 0x01) !== 0) {
        $clients[$index]['parts'][] = $frame['body'];

        return;
    }

    $parts = $clients[$index]['parts'];
    $parts[] = $frame['body'];
    $clients[$index]['parts'] = [];
    $body = implode('', $parts);
    $type = $clients[$index]['type'];

    if ($type === 'SUB') {
        if ($body === '') {
            return;
        }
        $action = $body[0];
        $topic = substr($body, 1);
        if ($action === "\x01") {
            $clients[$index]['subscriptions'][$topic] = true;
        } elseif ($action === "\x00") {
            unset($clients[$index]['subscriptions'][$topic]);
        }

        return;
    }

    if ($type === 'PUB') {
        foreach ($clients as $peer) {
            if ($peer['type'] !== 'SUB') {
                continue;
            }
            foreach (array_keys($peer['subscriptions']) as $topic) {
                if (str_starts_with($body, (string) $topic)) {
                    writeAll($peer['stream'], zmtpFrame(0x00, $body));
                    break;
                }
            }
        }

        return;
    }

    if ($type === 'PUSH') {
        foreach ($clients as $peer) {
            if ($peer['type'] === 'PULL') {
                writeAll($peer['stream'], zmtpFrame(0x00, $body));
            }
        }
    }
}

/**
 * Extracts one complete frame from a receive buffer.
 *
 * @return array{flags: int, body: string}|null
 */
function takeFrame(string &$buffer): ?array
{
    $length = strlen($buffer);
    if ($length < 2) {
        return null;
    }

    $flags = ord($buffer[0]);
    if (($flags & 0x02) !== 0) {
        if ($length < 10) {
            return null;
        }
        $unpacked = unpack('J', substr($buffer, 1, 8));
        $size = is_array($unpacked) ? (int) $unpacked[1] : -1;
        $offset = 9;
    } else {
        $size = ord($buffer[1]);
        $offset = 2;
    }

    if ($size < 0 || $length < $offset + $size) {
        return null;
    }

    $body = substr($buffer, $offset, $size);
    $buffer = substr($buffer, $offset + $size);

    return ['flags' => $flags, 'body' => $body];
}

function zmtpGreeting(): string
{
    return "\xFF" . str_repeat("\x00", 8) . "\x7F"
        . "\x03\x01"
        . str_pad('NULL', 20, "\x00")
        . "\x00"
        . str_repeat("\x00", 31);
}

function zmtpFrame(int $flags, string $body): string
{
    $length = strlen($body);
    if ($length > 0xFF) {
        return chr($flags | 0x02) . pack('J', $length) . $body;
    }

    return chr($flags) . chr($length) . $body;
}

function readyFrame(string $socketType): string
{
    $metadata = metadataProperty('Socket-Type', $socketType) . "\x00";

    return zmtpFrame(0x04, chr(5) . 'READY' . $metadata);
}

function metadataProperty(string $name, string $value): string
{
    return chr(strlen($name)) . $name . pack('N', strlen($value)) . $value;
}

/**
 * @return array<string, string>
 */
function parseMetadata(string $data): array
{
    $properties = [];
    $offset = 0;
    $length = strlen($data);

    while ($offset < $length) {
        $nameLength = ord($data[$offset]);
        $offset++;
        if ($nameLength === 0) {
            break;
        }
        if ($offset + $nameLength + 4 > $length) {
            throw new RuntimeException('Malformed metadata name.');
        }
        $name = substr($data, $offset, $nameLength);
        $offset += $nameLength;
        $unpacked = unpack('N', substr($data, $offset, 4));
        $valueLength = is_array($unpacked) ? (int) $unpacked[1] : 0;
        $offset += 4;
        if ($offset + $valueLength > $length) {
            throw new RuntimeException('Malformed metadata value.');
        }
        $properties[$name] = substr($data, $offset, $valueLength);
        $offset += $valueLength;
    }

    return $properties;
}

function compatiblePeerType(string $clientType): string
{
    $map = ['PUB' => 'SUB', 'SUB' => 'PUB', 'PUSH' => 'PULL', 'PULL' => 'PUSH'];
    if (!isset($map[$clientType])) {
        fwrite(STDERR, sprintf("Unsupported socket type [%s].\n", $clientType));
        exit(1);
    }

    return $map[$clientType];
}
