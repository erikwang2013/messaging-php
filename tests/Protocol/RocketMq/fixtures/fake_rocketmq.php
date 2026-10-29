<?php

declare(strict_types=1);

/**
 * Minimal RocketMQ nameserver + broker fixture for protocol tests.
 *
 * Usage: php fake_rocketmq.php <scenario> <log-file>
 * Prints "READY <nameserver-port> <broker-port>" on stdout.
 *
 * Scenarios: roundtrip, timeout (no SEND_MESSAGE reply), malformed (bad SEND_MESSAGE
 * reply frame), pull_timeout (no PULL_MESSAGE reply).
 */

if ($argc < 3) {
    fwrite(STDERR, "Usage: php fake_rocketmq.php <scenario> <log-file>\n");
    exit(1);
}

$scenario = $argv[1];
$logFile = $argv[2];

$nameserver = stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
$broker = stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
if ($nameserver === false || $broker === false) {
    fwrite(STDERR, "Unable to start fixture listeners: {$errorMessage}\n");
    exit(1);
}

stream_set_blocking($nameserver, false);
stream_set_blocking($broker, false);

$nameserverPort = (int) substr(strrchr((string) stream_socket_get_name($nameserver, false), ':'), 1);
$brokerPort = (int) substr(strrchr((string) stream_socket_get_name($broker, false), ':'), 1);

/** @var array<string, array<int, array{body: string, properties: string, physical: int, reconsume: int}>> */
$store = [];
$physical = 1000;

fwrite(STDOUT, sprintf("READY %d %d\n", $nameserverPort, $brokerPort));
fflush(STDOUT);

/** @param array<string, string> $extFields */
function reply(int $opaque, int $code, array $extFields = [], string $body = '', ?string $remark = null): string
{
    $header = [
        'code' => $code,
        'language' => 'JAVA',
        'version' => 407,
        'opaque' => $opaque,
        'flag' => 1,
    ];
    if ($extFields !== []) {
        $header['extFields'] = $extFields;
    }
    if ($remark !== null) {
        $header['remark'] = $remark;
    }

    $json = (string) json_encode($header);

    return pack('NN', 4 + strlen($json) + strlen($body), strlen($json)) . $json . $body;
}

/**
 * @return array<string, mixed>
 */
function routeBody(int $brokerPort): string
{
    return (string) json_encode([
        'orderTopicConf' => null,
        'queueDatas' => [
            ['brokerName' => 'broker-a', 'readQueueNums' => 1, 'writeQueueNums' => 1, 'perm' => 6, 'topicSynFlag' => 0],
        ],
        'brokerDatas' => [
            [
                'cluster' => 'DefaultCluster',
                'brokerName' => 'broker-a',
                'brokerAddrs' => ['0' => sprintf('127.0.0.1:%d', $brokerPort)],
            ],
        ],
        'filterServerTable' => [],
    ]);
}

/**
 * @return array<string, string>
 */
function parseProperties(string $raw): array
{
    $properties = [];
    foreach (explode("\x02", $raw) as $segment) {
        $separator = strpos($segment, "\x01");
        if ($separator !== false && $separator > 0) {
            $properties[substr($segment, 0, $separator)] = substr($segment, $separator + 1);
        }
    }

    return $properties;
}

/**
 * @param array<string, string> $properties
 */
function encodeProperties(array $properties): string
{
    $parts = [];
    foreach ($properties as $name => $value) {
        $parts[] = $name . "\x01" . $value;
    }

    return implode("\x02", $parts);
}

function storedRecord(
    string $topic,
    string $body,
    string $properties,
    int $queueOffset,
    int $physicalOffset,
    int $reconsumeTimes,
): string {
    $rest = pack('N', 0xDAA320A7)
        . pack('N', 0)
        . pack('N', 0)
        . pack('N', 0)
        . pack('J', $queueOffset)
        . pack('J', $physicalOffset)
        . pack('N', 0)
        . pack('J', (int) floor(microtime(true) * 1000))
        . str_repeat("\0", 8)
        . pack('J', (int) floor(microtime(true) * 1000))
        . str_repeat("\0", 8)
        . pack('N', $reconsumeTimes)
        . pack('J', 0)
        . pack('N', strlen($body)) . $body
        . chr(strlen($topic)) . $topic
        . pack('n', strlen($properties)) . $properties;

    return pack('N', 4 + strlen($rest)) . $rest;
}

function logEvent(string $logFile, string $line): void
{
    file_put_contents($logFile, $line . "\n", FILE_APPEND | LOCK_EX);
}

/**
 * @param array<string, mixed> $header
 */
function handleClient($client, string $type, array $header, string $body, array &$store, int &$physical, int $brokerPort, string $scenario, string $logFile): void
{
    $code = (int) $header['code'];
    $opaque = (int) $header['opaque'];
    $ext = is_array($header['extFields'] ?? null) ? $header['extFields'] : [];

    if ($type === 'ns') {
        logEvent($logFile, sprintf('ns:%d topic=%s', $code, $ext['topic'] ?? '-'));

        if ($code === 105) {
            fwrite($client, reply($opaque, 0, [], routeBody($brokerPort)));
            fflush($client);
        }

        return;
    }

    switch ($code) {
        case 34: // HEART_BEAT
            logEvent($logFile, 'broker:34');
            fwrite($client, reply($opaque, 0));
            fflush($client);

            return;

        case 30: // GET_MAX_OFFSET
        case 31: // GET_MIN_OFFSET
            $topic = (string) ($ext['topic'] ?? '');
            $count = count($store[$topic] ?? []);
            $offset = $code === 30 ? $count : 0;
            logEvent($logFile, sprintf('broker:%d topic=%s offset=%d', $code, $topic, $offset));
            fwrite($client, reply($opaque, 0, ['offset' => (string) $offset]));
            fflush($client);

            return;

        case 10: // SEND_MESSAGE
            $topic = (string) ($ext['topic'] ?? '');
            $properties = (string) ($ext['properties'] ?? '');
            $parsed = parseProperties($properties);
            logEvent($logFile, sprintf(
                'broker:10 topic=%s queue=%s key=%s delay=%s uniq=%s',
                $topic,
                $ext['queueId'] ?? '-',
                $parsed['KEYS'] ?? '-',
                $parsed['DELAY'] ?? '-',
                $parsed['UNIQ_KEY'] ?? '-',
            ));

            if ($scenario === 'timeout') {
                return;
            }
            if ($scenario === 'malformed') {
                // Declared header length exceeds the frame payload.
                fwrite($client, pack('NN', 20, 50) . str_repeat('x', 16));
                fflush($client);

                return;
            }

            $queueOffset = count($store[$topic] ?? []);
            $store[$topic][] = [
                'body' => $body,
                'properties' => $properties,
                'physical' => $physical,
                'reconsume' => 0,
            ];
            $physical++;

            fwrite($client, reply($opaque, 0, [
                'msgId' => 'msg-' . $queueOffset,
                'queueId' => (string) ($ext['queueId'] ?? 0),
                'queueOffset' => (string) $queueOffset,
            ]));
            fflush($client);

            return;

        case 11: // PULL_MESSAGE
            $topic = (string) ($ext['topic'] ?? '');
            $queueOffset = (int) ($ext['queueOffset'] ?? 0);
            logEvent($logFile, sprintf(
                'broker:11 topic=%s queue=%s offset=%d',
                $topic,
                $ext['queueId'] ?? '-',
                $queueOffset,
            ));

            if ($scenario === 'pull_timeout') {
                return;
            }

            $messages = $store[$topic] ?? [];
            $count = count($messages);
            if ($queueOffset >= $count) {
                fwrite($client, reply($opaque, 19, [
                    'nextBeginOffset' => (string) $count,
                    'minOffset' => '0',
                    'maxOffset' => (string) $count,
                    'suggestWhichBrokerId' => '0',
                ]));
                fflush($client);

                return;
            }

            $max = (int) ($ext['maxMsgNums'] ?? 1);
            $body = '';
            $next = $queueOffset;
            for ($i = $queueOffset; $i < min($count, $queueOffset + $max); $i++) {
                $message = $messages[$i];
                $body .= storedRecord($topic, $message['body'], $message['properties'], $i, $message['physical'], $message['reconsume']);
                $next = $i + 1;
            }

            fwrite($client, reply($opaque, 0, [
                'nextBeginOffset' => (string) $next,
                'minOffset' => '0',
                'maxOffset' => (string) $count,
                'suggestWhichBrokerId' => '0',
            ], $body));
            fflush($client);

            return;

        case 15: // UPDATE_CONSUMER_OFFSET
            logEvent($logFile, sprintf(
                'broker:15 topic=%s queue=%s commit=%s',
                $ext['topic'] ?? '-',
                $ext['queueId'] ?? '-',
                $ext['commitOffset'] ?? '-',
            ));
            fwrite($client, reply($opaque, 0));
            fflush($client);

            return;

        case 36: // CONSUMER_SEND_MSG_BACK
            $offset = (int) ($ext['offset'] ?? -1);
            $originTopic = (string) ($ext['originTopic'] ?? '');
            $group = (string) ($ext['group'] ?? '');
            logEvent($logFile, sprintf(
                'broker:36 offset=%d delay=%s origin=%s',
                $offset,
                $ext['delayLevel'] ?? '-',
                $originTopic,
            ));

            $source = null;
            $sourceTopic = null;
            foreach ($store as $topic => $messages) {
                foreach ($messages as $message) {
                    if ($message['physical'] === $offset) {
                        $source = $message;
                        $sourceTopic = $topic;
                        break 2;
                    }
                }
            }

            if ($source === null) {
                fwrite($client, reply($opaque, 22, [], '', 'message not found'));
                fflush($client);

                return;
            }

            $properties = parseProperties($source['properties']);
            $properties['RETRY_TOPIC'] = $originTopic !== '' ? $originTopic : (string) $sourceTopic;
            $retryTopic = '%RETRY%' . $group;
            if (!isset($store[$retryTopic])) {
                $store[$retryTopic] = [];
            }
            $store[$retryTopic][] = [
                'body' => $source['body'],
                'properties' => encodeProperties($properties),
                'physical' => $physical,
                'reconsume' => $source['reconsume'] + 1,
            ];
            $physical++;

            fwrite($client, reply($opaque, 0));
            fflush($client);

            return;
    }
}

$clients = [];

while (true) {
    $read = [$nameserver, $broker];
    foreach ($clients as $client) {
        $read[] = $client['socket'];
    }

    $write = null;
    $except = null;
    if (stream_select($read, $write, $except, 0, 200000) === false) {
        exit(1);
    }

    foreach ($read as $socket) {
        if ($socket === $nameserver || $socket === $broker) {
            $accepted = @stream_socket_accept($socket, 0);
            if ($accepted === false) {
                continue;
            }
            stream_set_blocking($accepted, false);
            $clients[(int) $accepted] = [
                'socket' => $accepted,
                'type' => $socket === $nameserver ? 'ns' : 'broker',
                'buffer' => '',
            ];
            continue;
        }

        $id = (int) $socket;
        $chunk = fread($socket, 65536);
        if ($chunk === false || ($chunk === '' && feof($socket))) {
            fclose($socket);
            unset($clients[$id]);
            continue;
        }

        $clients[$id]['buffer'] .= $chunk;

        while (strlen($clients[$id]['buffer']) >= 4) {
            $length = unpack('N', substr($clients[$id]['buffer'], 0, 4));
            if ($length === false) {
                break;
            }
            $length = (int) $length[1];
            if ($length < 8 || $length > 67108864) {
                fclose($socket);
                unset($clients[$id]);
                continue 2;
            }
            if (strlen($clients[$id]['buffer']) < 4 + $length) {
                break;
            }

            $frame = substr($clients[$id]['buffer'], 0, 4 + $length);
            $clients[$id]['buffer'] = substr($clients[$id]['buffer'], 4 + $length);

            $headerLength = unpack('N', substr($frame, 4, 4));
            if ($headerLength === false) {
                continue;
            }
            $headerLength = (int) $headerLength[1] & 0xFFFFFF;
            $header = json_decode(substr($frame, 8, $headerLength), true);
            if (!is_array($header)) {
                continue;
            }

            handleClient(
                $socket,
                $clients[$id]['type'],
                $header,
                substr($frame, 8 + $headerLength),
                $store,
                $physical,
                $brokerPort,
                $scenario,
                $logFile,
            );
        }
    }
}
