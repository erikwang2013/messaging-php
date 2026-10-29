<?php

declare(strict_types=1);

/*
 * Minimal scripted AMQP 0-9-1 server used by RabbitMqProtocolTest.
 *
 * Usage: php fake_amqp_broker.php <mode> <log-file>
 * Modes: normal, heartbeat, malformed, auth-error.
 *
 * Prints the listening port on stdout, accepts a single client and logs every
 * received frame as a base64 line to <log-file>.
 */

use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\Codec;
use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\ContentHeader;
use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\FieldTable;
use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\Frame;
use Erikwang2013\Messaging\Driver\RabbitMq\Amqp\Protocol;

require __DIR__ . '/../../../vendor/autoload.php';

$mode = $argv[1] ?? 'normal';
$logFile = $argv[2] ?? '';

$server = @stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
if ($server === false) {
    fwrite(STDERR, sprintf("fake AMQP server failed to listen: %s (%d)\n", $errorMessage, $errorCode));
    exit(1);
}

$address = stream_socket_get_name($server, false);
if (!is_string($address)) {
    exit(1);
}
$separator = strrpos($address, ':');
fwrite(STDOUT, substr($address, (int) $separator + 1) . "\n");
fflush(STDOUT);

$client = @stream_socket_accept($server, 15);
if ($client === false) {
    exit(1);
}
stream_set_timeout($client, 15);

$log = static function (string $raw) use ($logFile): void {
    if ($logFile !== '') {
        file_put_contents($logFile, base64_encode($raw) . "\n", FILE_APPEND | LOCK_EX);
    }
};

$readExact = static function (int $length) use ($client): ?string {
    $buffer = '';
    while (strlen($buffer) < $length) {
        $chunk = @fread($client, $length - strlen($buffer));
        if ($chunk === false || $chunk === '') {
            return null;
        }
        $buffer .= $chunk;
    }

    return $buffer;
};

/**
 * @return array{type: int, channel: int, payload: string}|null
 */
$readFrame = static function () use ($client, $readExact, $log): ?array {
    $header = $readExact(Frame::HEADER_SIZE);
    if ($header === null) {
        return null;
    }

    $meta = Frame::decodeHeader($header);
    $payload = $meta['size'] > 0 ? $readExact($meta['size']) : '';
    $end = $readExact(1);
    if ($payload === null || $end !== Frame::END) {
        return null;
    }

    $log(Frame::encode($meta['type'], $meta['channel'], $payload));

    return ['type' => $meta['type'], 'channel' => $meta['channel'], 'payload' => $payload];
};

$sendFrame = static function (int $type, int $channel, string $payload = '') use ($client): void {
    @fwrite($client, Frame::encode($type, $channel, $payload));
};

$sendMethod = static function (int $channel, int $classId, int $methodId, string $args = '') use ($sendFrame): void {
    $sendFrame(Frame::TYPE_METHOD, $channel, Protocol::methodPayload($classId, $methodId, $args));
};

$malformed = static function (int $channel) use ($client): void {
    $frame = Frame::encode(Frame::TYPE_METHOD, $channel, Protocol::methodPayload(Protocol::CLASS_QUEUE, Protocol::QUEUE_DECLARE_OK, Codec::shortstr('q') . Codec::long(0) . Codec::long(0)));
    @fwrite($client, substr($frame, 0, -1) . "\x00");
};

$header = $readExact(8);
if ($header !== Protocol::PROTOCOL_HEADER) {
    exit(1);
}

$sendMethod(
    0,
    Protocol::CLASS_CONNECTION,
    Protocol::CONNECTION_START,
    "\x00\x09"
    . FieldTable::encode(['product' => 'fake-rabbit', 'version' => '0.1'])
    . Codec::longstr('PLAIN AMQPLAIN')
    . Codec::longstr('en_US'),
);

$startOk = $readFrame();
if ($startOk === null) {
    exit(1);
}

if ($mode === 'auth-error') {
    $sendMethod(
        0,
        Protocol::CLASS_CONNECTION,
        Protocol::CONNECTION_CLOSE,
        Codec::short(403) . Codec::shortstr('ACCESS_REFUSED - Login was refused using authentication mechanism PLAIN.') . Codec::short(0) . Codec::short(0),
    );
    $readFrame();
    exit(0);
}

$sendMethod(
    0,
    Protocol::CLASS_CONNECTION,
    Protocol::CONNECTION_TUNE,
    Codec::short(2047) . Codec::long(131072) . Codec::short(60),
);

$tuneOk = $readFrame();
$open = $readFrame();
if ($tuneOk === null || $open === null) {
    exit(1);
}
$sendMethod(0, Protocol::CLASS_CONNECTION, Protocol::CONNECTION_OPEN_OK, Codec::shortstr(''));

/** @var array<string, string> $consumerByQueue */
$consumerByQueue = [];
/** @var array<int, array{body: string, header: string, routing_key: string, consumer_tag: string}> $pending */
$pending = [];
/** @var array<string, list<array{body: string, header: string, routing_key: string}>> $queued */
$queued = [];
$deliveryTag = 0;

$deliver = static function (
    int $channel,
    string $tag,
    string $routingKey,
    string $header,
    string $body,
    bool $redelivered,
) use (&$deliveryTag, &$pending, $sendMethod, $sendFrame): void {
    $deliveryTag++;
    $pending[$deliveryTag] = [
        'body' => $body,
        'header' => $header,
        'routing_key' => $routingKey,
        'consumer_tag' => $tag,
    ];

    $sendMethod(
        $channel,
        Protocol::CLASS_BASIC,
        Protocol::BASIC_DELIVER,
        Codec::shortstr($tag)
        . Codec::longlong($deliveryTag)
        . Codec::bits([$redelivered])
        . Codec::shortstr('')
        . Codec::shortstr($routingKey),
    );
    $sendFrame(Frame::TYPE_HEADER, $channel, $header);
    if ($body !== '') {
        $sendFrame(Frame::TYPE_BODY, $channel, $body);
    }
};

while (true) {
    if ($mode === 'heartbeat') {
        $read = [$client];
        $write = null;
        $except = null;
        if (@stream_select($read, $write, $except, 0, 30000) === 0) {
            $sendFrame(Frame::TYPE_HEARTBEAT, 0);
            continue;
        }
    }

    $frame = $readFrame();
    if ($frame === null) {
        exit(0);
    }

    if ($frame['type'] === Frame::TYPE_HEARTBEAT) {
        continue;
    }

    if ($frame['type'] !== Frame::TYPE_METHOD) {
        exit(1);
    }

    $method = Protocol::decodeMethod($frame['payload']);
    $channel = $frame['channel'];
    $classId = $method['class_id'];
    $methodId = $method['method_id'];
    $args = $method['args'];

    if ($classId === Protocol::CLASS_CHANNEL && $methodId === Protocol::CHANNEL_OPEN) {
        $sendMethod($channel, Protocol::CLASS_CHANNEL, Protocol::CHANNEL_OPEN_OK, Codec::longstr(''));
        continue;
    }

    if ($classId === Protocol::CLASS_CONNECTION && $methodId === Protocol::CONNECTION_CLOSE) {
        $sendMethod(0, Protocol::CLASS_CONNECTION, Protocol::CONNECTION_CLOSE_OK);
        exit(0);
    }

    if ($classId === Protocol::CLASS_CHANNEL && $methodId === Protocol::CHANNEL_CLOSE) {
        $sendMethod($channel, Protocol::CLASS_CHANNEL, Protocol::CHANNEL_CLOSE_OK);
        continue;
    }

    if ($mode === 'malformed') {
        $malformed($channel);
        exit(0);
    }

    if ($classId === Protocol::CLASS_QUEUE && $methodId === Protocol::QUEUE_DECLARE) {
        $queue = (string) ($args['queue'] ?? '');
        $sendMethod(
            $channel,
            Protocol::CLASS_QUEUE,
            Protocol::QUEUE_DECLARE_OK,
            Codec::shortstr($queue) . Codec::long(0) . Codec::long(0),
        );
        continue;
    }

    if ($classId === Protocol::CLASS_BASIC && $methodId === Protocol::BASIC_QOS) {
        $sendMethod($channel, Protocol::CLASS_BASIC, Protocol::BASIC_QOS_OK);
        continue;
    }

    if ($classId === Protocol::CLASS_BASIC && $methodId === Protocol::BASIC_CONSUME) {
        $queue = (string) ($args['queue'] ?? '');
        $tag = (string) ($args['consumer_tag'] ?? '');
        $consumerByQueue[$queue] = $tag;
        $sendMethod($channel, Protocol::CLASS_BASIC, Protocol::BASIC_CONSUME_OK, Codec::shortstr($tag));
        foreach ($queued[$queue] ?? [] as $queuedMessage) {
            $deliver($channel, $tag, $queuedMessage['routing_key'], $queuedMessage['header'], $queuedMessage['body'], false);
        }
        unset($queued[$queue]);
        continue;
    }

    if ($classId === Protocol::CLASS_BASIC && $methodId === Protocol::BASIC_PUBLISH) {
        $headerFrame = $readFrame();
        if ($headerFrame === null || $headerFrame['type'] !== Frame::TYPE_HEADER) {
            exit(1);
        }

        $header = ContentHeader::decode($headerFrame['payload']);
        $bodySize = (int) $header['body_size'];
        $body = '';
        while (strlen($body) < $bodySize) {
            $bodyFrame = $readFrame();
            if ($bodyFrame === null || $bodyFrame['type'] !== Frame::TYPE_BODY) {
                exit(1);
            }
            $body .= $bodyFrame['payload'];
        }

        $routingKey = (string) ($args['routing_key'] ?? '');
        $tag = $consumerByQueue[$routingKey] ?? null;
        if ($tag === null) {
            $queued[$routingKey][] = [
                'body' => $body,
                'header' => $headerFrame['payload'],
                'routing_key' => $routingKey,
            ];
            continue;
        }

        $deliver($channel, $tag, $routingKey, $headerFrame['payload'], $body, false);
        continue;
    }

    if ($classId === Protocol::CLASS_BASIC && $methodId === Protocol::BASIC_ACK) {
        unset($pending[(int) ($args['delivery_tag'] ?? 0)]);
        continue;
    }

    if ($classId === Protocol::CLASS_BASIC && $methodId === Protocol::BASIC_NACK) {
        $tag = (int) ($args['delivery_tag'] ?? 0);
        $requeue = (bool) ($args['requeue'] ?? false);
        $message = $pending[$tag] ?? null;
        unset($pending[$tag]);

        if ($requeue && $message !== null) {
            $deliver($channel, $message['consumer_tag'], $message['routing_key'], $message['header'], $message['body'], true);
        }
        continue;
    }

    if ($classId === Protocol::CLASS_CONNECTION && $methodId === Protocol::CONNECTION_CLOSE_OK) {
        exit(0);
    }
}

exit(0);
