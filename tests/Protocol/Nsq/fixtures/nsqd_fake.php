<?php

declare(strict_types=1);

/**
 * Minimal scripted NSQ v2 server for protocol tests.
 *
 * Usage: php nsqd_fake.php <scenario> <log-file>
 * Prints the listening port on stdout, accepts one client, logs every
 * command it receives to the log file, then exits.
 */

$scenario = $argv[1] ?? 'publish';
$logFile = $argv[2] ?? null;

$server = stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
if ($server === false) {
    fwrite(STDERR, sprintf("nsqd_fake: listen failed: %s\n", $errorMessage));
    exit(1);
}

$address = stream_socket_get_name($server, false);
if ($address === false) {
    exit(1);
}

$port = (int) substr($address, strrpos($address, ':') + 1);
fwrite(STDOUT, $port . "\n");
fflush(STDOUT);

$client = stream_socket_accept($server, 10.0);
if ($client === false) {
    exit(1);
}

$log = static function (string $event) use ($logFile): void {
    if ($logFile !== null) {
        file_put_contents($logFile, $event . "\n", FILE_APPEND | LOCK_EX);
    }
};

/**
 * @param resource $stream
 */
$readExactly = static function ($stream, int $length): string {
    $data = '';
    while (strlen($data) < $length) {
        $chunk = fread($stream, $length - strlen($data));
        if ($chunk === false || $chunk === '') {
            break;
        }
        $data .= $chunk;
    }

    return $data;
};

/**
 * @param resource $stream
 */
$readCommand = static function ($stream) use ($log): string {
    $line = fgets($stream);
    $line = $line === false ? '' : rtrim($line, "\r\n");
    $log('command:' . $line);

    return $line;
};

$frame = static fn (int $type, string $payload): string => pack('NN', 4 + strlen($payload), $type) . $payload;

$messagePayload = static fn (): string => pack('J', 1234567890123)
    . pack('n', 3)
    . '0123456789abcdef'
    . 'hello';

$magic = $readExactly($client, 4);
$log('magic:' . $magic);

switch ($scenario) {
    case 'publish':
        $readCommand($client);
        $length = unpack('N', $readExactly($client, 4))[1] ?? 0;
        $log('body:' . $readExactly($client, $length));
        fwrite($client, $frame(0, 'OK'));
        break;

    case 'error':
        $readCommand($client);
        $length = unpack('N', $readExactly($client, 4))[1] ?? 0;
        $readExactly($client, $length);
        fwrite($client, $frame(1, 'E_INVALID fake failure'));
        break;

    case 'receive':
        $readCommand($client); // SUB <topic> <channel>
        fwrite($client, $frame(0, 'OK'));
        $readCommand($client); // RDY 1
        fwrite($client, $frame(2, $messagePayload()));
        $readCommand($client); // FIN or REQ
        break;

    case 'heartbeat':
        $readCommand($client); // SUB
        fwrite($client, $frame(0, 'OK'));
        $readCommand($client); // RDY 1
        fwrite($client, $frame(0, '_heartbeat_'));
        $readCommand($client); // NOP
        fwrite($client, $frame(2, $messagePayload()));
        $readCommand($client); // FIN
        break;

    case 'partial':
        $readCommand($client); // SUB
        fwrite($client, $frame(0, 'OK'));
        $readCommand($client); // RDY 1
        $encoded = $frame(2, $messagePayload());
        fwrite($client, substr($encoded, 0, 5));
        usleep(150_000);
        fwrite($client, substr($encoded, 5));
        $readCommand($client); // FIN
        break;

    case 'reconnect':
        // Drop the first connection without replying, then serve the retry on a second one.
        $readCommand($client);
        $length = unpack('N', $readExactly($client, 4))[1] ?? 0;
        $log('body:' . $readExactly($client, $length));
        fclose($client);
        $client = stream_socket_accept($server, 10.0);
        if ($client === false) {
            exit(1);
        }
        $log('magic:' . $readExactly($client, 4));
        $readCommand($client);
        $length = unpack('N', $readExactly($client, 4))[1] ?? 0;
        $log('body:' . $readExactly($client, $length));
        fwrite($client, $frame(0, 'OK'));
        break;

    case 'timeout':
        $readCommand($client); // SUB
        fwrite($client, $frame(0, 'OK'));
        $readCommand($client); // RDY 1
        usleep(800_000);
        break;

    default:
        fwrite(STDERR, sprintf("nsqd_fake: unknown scenario [%s]\n", $scenario));
        exit(1);
}

fclose($client);
fclose($server);
