<?php

declare(strict_types=1);

/**
 * Minimal scripted AWS HTTP endpoint for protocol tests.
 *
 * Usage: php aws_fake.php <scenario> <log-file> [max-requests]
 * Prints the listening port on stdout, answers `max-requests` HTTP requests
 * (one per connection), logging each request as one JSON line.
 */

$scenario = $argv[1] ?? 'sqs';
$logFile = $argv[2] ?? null;
$maxRequests = (int) ($argv[3] ?? 10);

$server = stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
if ($server === false) {
    fwrite(STDERR, sprintf("aws_fake: listen failed: %s\n", $errorMessage));
    exit(1);
}

$address = stream_socket_get_name($server, false);
if ($address === false) {
    exit(1);
}

fwrite(STDOUT, (int) substr($address, strrpos($address, ':') + 1) . "\n");
fflush(STDOUT);

$log = static function (array $entry) use ($logFile): void {
    if ($logFile !== null) {
        file_put_contents($logFile, json_encode($entry) . "\n", FILE_APPEND | LOCK_EX);
    }
};

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

$respond = static function ($client, int $status, string $contentType, string $body): void {
    $reason = $status === 200 ? 'OK' : 'Error';
    fwrite($client, sprintf(
        "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
        $status,
        $reason,
        $contentType,
        strlen($body),
        $body,
    ));
};

$json = static function (array $data, int $status = 200): array {
    return [$status, 'application/x-amz-json-1.0', json_encode($data)];
};

$formParams = static function (string $body): array {
    $params = [];
    foreach (explode('&', $body) as $pair) {
        if ($pair === '') {
            continue;
        }

        $parts = explode('=', $pair, 2);
        $params[rawurldecode($parts[0])] = isset($parts[1]) ? rawurldecode($parts[1]) : '';
    }

    return $params;
};

$sqsMessage = [
    'MessageId' => 'sqs-msg-1',
    'ReceiptHandle' => 'rh-1',
    'Body' => 'hello',
    'MessageGroupId' => 'group-1',
    'Attributes' => ['ApproximateReceiveCount' => '2'],
    'MessageAttributes' => [
        'trace' => ['DataType' => 'String', 'StringValue' => 'abc'],
        'blob' => ['DataType' => 'Binary', 'BinaryValue' => base64_encode("\x00\xff")],
    ],
];

$servedReceives = 0;
$lastKinesisRecord = null;
$servedKinesisRecords = false;

for ($served = 0; $served < $maxRequests; $served++) {
    $client = stream_socket_accept($server, 5.0);
    if ($client === false) {
        break;
    }

    $requestLine = fgets($client);
    if ($requestLine === false) {
        fclose($client);
        continue;
    }

    $requestLine = rtrim($requestLine, "\r\n");
    $parts = explode(' ', $requestLine);
    $method = $parts[0] ?? '';
    $path = $parts[1] ?? '/';

    $headers = [];
    while (($line = fgets($client)) !== false) {
        $line = rtrim($line, "\r\n");
        if ($line === '') {
            break;
        }

        $separator = strpos($line, ':');
        if ($separator !== false) {
            $headers[strtolower(trim(substr($line, 0, $separator)))] = trim(substr($line, $separator + 1));
        }
    }

    $length = (int) ($headers['content-length'] ?? 0);
    $body = $length > 0 ? $readExactly($client, $length) : '';
    $target = $headers['x-amz-target'] ?? '';
    $action = strpos($target, '.') === false ? '' : substr($target, strpos($target, '.') + 1);
    $form = $formParams($body);

    $log([
        'method' => $method,
        'path' => $path,
        'target' => $target,
        'action' => $action !== '' ? $action : ($form['Action'] ?? ''),
        'authorization' => $headers['authorization'] ?? '',
        'body' => $body,
    ]);

    if ($scenario === 'sqs_error') {
        [$status, $type, $payload] = $json([
            '__type' => 'com.amazonaws.sqs#QueueDoesNotExist',
            'message' => 'The specified queue does not exist.',
        ], 400);
        $respond($client, $status, $type, $payload);
        fclose($client);
        continue;
    }

    if ($scenario === 'sns_error') {
        $respond($client, 400, 'text/xml', '<ErrorResponse><Error><Type>Sender</Type><Code>InvalidParameter</Code><Message>bad request</Message></Error></ErrorResponse>');
        fclose($client);
        continue;
    }

    if ($scenario === 'kinesis_error') {
        [$status, $type, $payload] = $json([
            '__type' => 'ResourceNotFoundException',
            'message' => 'Stream not found.',
        ], 400);
        $respond($client, $status, $type, $payload);
        fclose($client);
        continue;
    }

    if (str_starts_with($target, 'AmazonSQS.')) {
        $data = json_decode($body, true);
        $data = is_array($data) ? $data : [];

        switch ($action) {
            case 'GetQueueUrl':
                $host = $headers['host'] ?? '127.0.0.1';
                $queue = is_string($data['QueueName'] ?? null) ? $data['QueueName'] : 'events';
                [$status, $type, $payload] = $json(['QueueUrl' => 'http://' . $host . '/queue/' . $queue]);
                break;

            case 'SendMessage':
                [$status, $type, $payload] = $json(['MessageId' => 'sqs-msg-1', 'MD5OfMessageBody' => 'md5']);
                break;

            case 'SendMessageBatch':
                $successful = [];
                foreach (is_array($data['Entries'] ?? null) ? $data['Entries'] : [] as $entry) {
                    if (is_array($entry) && isset($entry['Id'])) {
                        $successful[] = ['Id' => (string) $entry['Id'], 'MessageId' => 'sqs-msg-' . $entry['Id']];
                    }
                }
                [$status, $type, $payload] = $json(['Successful' => $successful, 'Failed' => []]);
                break;

            case 'ReceiveMessage':
                $messages = [];
                if ($scenario !== 'sqs_empty' && $servedReceives === 0) {
                    $messages = [$sqsMessage];
                    $servedReceives++;
                } else {
                    $servedReceives++;
                }
                [$status, $type, $payload] = $json($messages === [] ? [] : ['Messages' => $messages]);
                break;

            case 'DeleteMessage':
            case 'ChangeMessageVisibility':
                [$status, $type, $payload] = $json([]);
                break;

            default:
                [$status, $type, $payload] = $json(['__type' => 'InvalidAction', 'message' => 'unknown action ' . $action], 400);
        }

        $respond($client, $status, $type, $payload);
        fclose($client);
        continue;
    }

    if (str_starts_with($target, 'Kinesis_20131202.')) {
        $data = json_decode($body, true);
        $data = is_array($data) ? $data : [];

        switch ($action) {
            case 'PutRecord':
                $lastKinesisRecord = [
                    'SequenceNumber' => 'seq-pub-1',
                    'Data' => is_string($data['Data'] ?? null) ? $data['Data'] : '',
                    'PartitionKey' => is_string($data['PartitionKey'] ?? null) ? $data['PartitionKey'] : '',
                ];
                [$status, $type, $payload] = $json(['SequenceNumber' => 'seq-pub-1', 'ShardId' => 'shard-0']);
                break;

            case 'PutRecords':
                $records = [];
                foreach (is_array($data['Records'] ?? null) ? $data['Records'] : [] as $index => $record) {
                    $records[] = ['SequenceNumber' => 'seq-' . $index, 'ShardId' => 'shard-0'];
                }
                [$status, $type, $payload] = $json(['FailedRecordCount' => 0, 'Records' => $records]);
                break;

            case 'ListShards':
                [$status, $type, $payload] = $json(['Shards' => [[
                    'ShardId' => 'shard-0',
                    'SequenceNumberRange' => ['StartingSequenceNumber' => '1'],
                ]]]);
                break;

            case 'GetShardIterator':
                [$status, $type, $payload] = $json(['ShardIterator' => 'it-1']);
                break;

            case 'GetRecords':
                $records = [];
                if ($scenario !== 'kinesis_empty' && !$servedKinesisRecords && $lastKinesisRecord !== null) {
                    $records = [$lastKinesisRecord];
                    $servedKinesisRecords = true;
                }
                [$status, $type, $payload] = $json(['Records' => $records, 'NextShardIterator' => 'it-next']);
                break;

            default:
                [$status, $type, $payload] = $json(['__type' => 'InvalidAction', 'message' => 'unknown action ' . $action], 400);
        }

        $respond($client, $status, $type, $payload);
        fclose($client);
        continue;
    }

    if ($form['Action'] ?? null) {
        if (($form['Action'] ?? '') === 'Publish') {
            $respond($client, 200, 'text/xml', '<PublishResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">'
                . '<PublishResult><MessageId>sns-msg-1</MessageId></PublishResult>'
                . '<ResponseMetadata><RequestId>sns-req-1</RequestId></ResponseMetadata>'
                . '</PublishResponse>');
        } else {
            $respond($client, 400, 'text/xml', '<ErrorResponse><Error><Code>InvalidAction</Code><Message>unknown action</Message></Error></ErrorResponse>');
        }
        fclose($client);
        continue;
    }

    $respond($client, 400, 'text/plain', 'unexpected request');
    fclose($client);
}

fclose($server);
