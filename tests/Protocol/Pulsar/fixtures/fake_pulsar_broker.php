<?php

declare(strict_types=1);

/**
 * Minimal scripted Pulsar broker for protocol tests.
 *
 * Usage: php fake_pulsar_broker.php <log-file> <scenario>
 *
 * Prints the listening port on stdout. The driver opens one connection per
 * producer/consumer, so the broker multiplexes all clients with stream_select.
 * Scenarios: basic, redirect, lookup_error, send_error, malformed, partial.
 */

require dirname(__DIR__, 4) . '/vendor/autoload.php';

use Erikwang2013\Messaging\Driver\Pulsar\Protobuf;
use Erikwang2013\Messaging\Driver\Pulsar\PulsarFrame;
use Erikwang2013\Messaging\Driver\Pulsar\PulsarProtocol;

final class FakePulsarBroker
{
    /** @var resource */
    private $primary;

    /** @var resource|null */
    private $secondary = null;

    private ?string $secondaryUrl = null;

    private string $primaryUrl = '';

    /** @var array<int, array{socket: resource, server: int, buffer: string}> */
    private array $clients = [];

    /** @var array<int, string> */
    private array $producers = [];

    /** @var array<int, array{client: int, topic: string, permits: int}> */
    private array $consumers = [];

    /**
     * @var array<string, list<array{id: string, label: string, frame: string, acked: bool, delivered_to: int|null, deliveries: int}>>
     */
    private array $topics = [];

    private int $nextClient = 1;

    private int $entry = 0;

    public function __construct(
        private string $scenario,
        private ?string $logFile,
    ) {
    }

    public function run(): void
    {
        $this->primary = $this->listen();
        $this->primaryUrl = 'pulsar://127.0.0.1:' . $this->port($this->primary);

        if ($this->scenario === 'redirect') {
            $this->secondary = $this->listen();
            $this->secondaryUrl = 'pulsar://127.0.0.1:' . $this->port($this->secondary);
        }

        fwrite(STDOUT, $this->port($this->primary) . "\n");
        fflush(STDOUT);

        $servers = [$this->primary];
        if ($this->secondary !== null) {
            $servers[] = $this->secondary;
        }

        while (true) {
            $read = $servers;
            foreach ($this->clients as $client) {
                $read[] = $client['socket'];
            }

            $write = null;
            $except = null;
            $selected = @stream_select($read, $write, $except, 5);
            if ($selected === false) {
                break;
            }

            foreach ($read as $socket) {
                $serverIndex = array_search($socket, $servers, true);
                if ($serverIndex !== false) {
                    $accepted = @stream_socket_accept($socket, 0);
                    if ($accepted === false) {
                        continue;
                    }

                    stream_set_blocking($accepted, false);
                    $this->clients[$this->nextClient++] = [
                        'socket' => $accepted,
                        'server' => $serverIndex,
                        'buffer' => '',
                    ];
                    continue;
                }

                foreach ($this->clients as $clientId => $client) {
                    if ($client['socket'] !== $socket) {
                        continue;
                    }

                    $chunk = @fread($socket, 65536);
                    if ($chunk === '' || $chunk === false) {
                        if (feof($socket)) {
                            fclose($socket);
                            unset($this->clients[$clientId]);
                        }

                        break;
                    }

                    $this->clients[$clientId]['buffer'] .= $chunk;
                    $this->process($clientId);
                    break;
                }
            }
        }
    }

    /**
     * @return resource
     */
    private function listen()
    {
        $socket = @stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
        if ($socket === false) {
            fwrite(STDERR, sprintf("fake_pulsar_broker: listen failed: %s\n", $errorMessage));
            exit(1);
        }

        stream_set_blocking($socket, false);

        return $socket;
    }

    /**
     * @param resource $socket
     */
    private function port($socket): int
    {
        $address = (string) stream_socket_get_name($socket, false);

        return (int) substr($address, strrpos($address, ':') + 1);
    }

    private function process(int $clientId): void
    {
        while (isset($this->clients[$clientId])) {
            $parsed = PulsarProtocol::parseFrame($this->clients[$clientId]['buffer']);
            if ($parsed === null) {
                return;
            }

            [$frame, $consumed] = $parsed;
            $this->clients[$clientId]['buffer'] = substr($this->clients[$clientId]['buffer'], $consumed);
            $this->handle($clientId, $frame);
        }
    }

    private function handle(int $clientId, PulsarFrame $frame): void
    {
        $type = $frame->type();

        switch ($type) {
            case PulsarProtocol::COMMAND_CONNECT:
                $this->log(['event' => 'command', 'command' => 'Connect']);

                if ($this->scenario === 'malformed') {
                    @fwrite($this->clients[$clientId]['socket'], pack('N', 3) . 'abc');

                    return;
                }

                $connected = $this->commandBytes(
                    PulsarProtocol::COMMAND_CONNECTED,
                    Protobuf::stringField(1, 'fake-pulsar')
                    . Protobuf::varintField(2, PulsarProtocol::PROTOCOL_VERSION),
                );

                if ($this->scenario === 'basic') {
                    @fwrite($this->clients[$clientId]['socket'], $this->commandBytes(PulsarProtocol::COMMAND_PING));
                }

                if ($this->scenario === 'partial') {
                    $split = 5;
                    @fwrite($this->clients[$clientId]['socket'], substr($connected, 0, $split));
                    fflush($this->clients[$clientId]['socket']);
                    usleep(30000);
                    @fwrite($this->clients[$clientId]['socket'], substr($connected, $split));

                    return;
                }

                @fwrite($this->clients[$clientId]['socket'], $connected);

                return;

            case PulsarProtocol::COMMAND_LOOKUP:
                $requestId = $frame->intField(2);
                $topic = $frame->field(1) ?? '';
                $authoritative = $frame->intField(3) === 1;
                $this->log([
                    'event' => 'command',
                    'command' => 'Lookup',
                    'topic' => $topic,
                    'authoritative' => $authoritative,
                ]);

                if ($this->scenario === 'redirect' && $this->clients[$clientId]['server'] === 0) {
                    $this->log(['event' => 'redirect', 'url' => $this->secondaryUrl]);
                    $this->send(
                        $clientId,
                        PulsarProtocol::COMMAND_LOOKUP_RESPONSE,
                        Protobuf::stringField(1, (string) $this->secondaryUrl)
                        . Protobuf::varintField(3, PulsarProtocol::LOOKUP_REDIRECT)
                        . Protobuf::varintField(4, $requestId)
                        . Protobuf::boolField(5, true)
                    );

                    return;
                }

                if ($this->scenario === 'lookup_error') {
                    $this->send(
                        $clientId,
                        PulsarProtocol::COMMAND_LOOKUP_RESPONSE,
                        Protobuf::varintField(3, PulsarProtocol::LOOKUP_FAILED)
                        . Protobuf::varintField(4, $requestId)
                        . Protobuf::varintField(6, 11)
                        . Protobuf::stringField(7, 'fake topic does not exist')
                    );

                    return;
                }

                $this->send(
                    $clientId,
                    PulsarProtocol::COMMAND_LOOKUP_RESPONSE,
                    Protobuf::stringField(1, $this->primaryUrl)
                    . Protobuf::varintField(3, PulsarProtocol::LOOKUP_CONNECT)
                    . Protobuf::varintField(4, $requestId)
                    . Protobuf::boolField(5, true)
                );

                return;

            case PulsarProtocol::COMMAND_PRODUCER:
                $topic = $frame->field(1) ?? '';
                $producerId = $frame->intField(2);
                $requestId = $frame->intField(3);
                $name = $frame->field(4) ?? 'fake-producer';
                $this->producers[$producerId] = $topic;
                $this->log(['event' => 'command', 'command' => 'Producer', 'topic' => $topic]);

                $this->send(
                    $clientId,
                    PulsarProtocol::COMMAND_PRODUCER_SUCCESS,
                    Protobuf::varintField(1, $requestId)
                    . Protobuf::stringField(2, $name)
                    . Protobuf::varintField(3, 0)
                );

                return;

            case PulsarProtocol::COMMAND_SEND:
                $producerId = $frame->intField(1);
                $sequenceId = $frame->intField(2);
                $topic = $this->producers[$producerId] ?? '';
                $payloadFrame = $frame->payload() ?? '';
                $delayAt = null;

                if ($payloadFrame !== '') {
                    [$metadata] = PulsarProtocol::decodePayload($payloadFrame);
                    $delayAt = Protobuf::intValue($metadata, 19, 0) ?: null;
                }

                $this->log([
                    'event' => 'command',
                    'command' => 'Send',
                    'topic' => $topic,
                    'sequence' => $sequenceId,
                    'delay_at' => $delayAt,
                ]);

                if ($this->scenario === 'send_error') {
                    $this->send(
                        $clientId,
                        PulsarProtocol::COMMAND_SEND_ERROR,
                        Protobuf::varintField(1, $producerId)
                        . Protobuf::varintField(2, $sequenceId)
                        . Protobuf::varintField(3, 6)
                        . Protobuf::stringField(4, 'fake broker overloaded')
                    );

                    return;
                }

                $this->entry++;
                $encodedId = Protobuf::varintField(1, 1) . Protobuf::varintField(2, $this->entry);
                $label = '1:' . $this->entry;
                $this->topics[$topic][] = [
                    'id' => $encodedId,
                    'label' => $label,
                    'frame' => $payloadFrame,
                    'acked' => false,
                    'delivered_to' => null,
                    'deliveries' => 0,
                ];

                $this->send(
                    $clientId,
                    PulsarProtocol::COMMAND_SEND_RECEIPT,
                    Protobuf::varintField(1, $producerId)
                    . Protobuf::varintField(2, $sequenceId)
                    . Protobuf::bytesField(3, $encodedId)
                );

                $this->deliver();

                return;

            case PulsarProtocol::COMMAND_SUBSCRIBE:
                $topic = $frame->field(1) ?? '';
                $consumerId = $frame->intField(4);
                $requestId = $frame->intField(5);
                $this->consumers[$consumerId] = [
                    'client' => $clientId,
                    'topic' => $topic,
                    'permits' => 0,
                ];
                $this->log([
                    'event' => 'command',
                    'command' => 'Subscribe',
                    'topic' => $topic,
                    'subscription' => $frame->field(2) ?? '',
                    'consumer' => $consumerId,
                ]);

                $this->send($clientId, PulsarProtocol::COMMAND_SUCCESS, Protobuf::varintField(1, $requestId));

                return;

            case PulsarProtocol::COMMAND_FLOW:
                $consumerId = $frame->intField(1);
                $permits = $frame->intField(2);
                $this->log([
                    'event' => 'command',
                    'command' => 'Flow',
                    'consumer' => $consumerId,
                    'permits' => $permits,
                ]);

                if (isset($this->consumers[$consumerId])) {
                    $this->consumers[$consumerId]['permits'] += $permits;
                    $this->deliver();
                }

                return;

            case PulsarProtocol::COMMAND_ACK:
                $consumerId = $frame->intField(1);
                $encodedId = $frame->field(3);
                $label = null;

                if ($encodedId !== null) {
                    $label = PulsarProtocol::messageIdString(PulsarProtocol::decodeMessageId($encodedId));
                    $this->acknowledge($consumerId, $label);
                }

                $this->log([
                    'event' => 'command',
                    'command' => 'Ack',
                    'consumer' => $consumerId,
                    'message_id' => $label,
                ]);

                return;

            case PulsarProtocol::COMMAND_REDELIVER:
                $consumerId = $frame->intField(1);
                $this->log(['event' => 'command', 'command' => 'Redeliver', 'consumer' => $consumerId]);

                if (isset($this->consumers[$consumerId])) {
                    $topic = $this->consumers[$consumerId]['topic'];
                    foreach ($this->topics[$topic] ?? [] as $index => $message) {
                        if (!$message['acked'] && $message['delivered_to'] === $consumerId) {
                            $this->topics[$topic][$index]['delivered_to'] = null;
                        }
                    }
                }

                return;

            case PulsarProtocol::COMMAND_CLOSE_PRODUCER:
                $this->log(['event' => 'command', 'command' => 'CloseProducer']);
                unset($this->producers[$frame->intField(1)]);

                return;

            case PulsarProtocol::COMMAND_CLOSE_CONSUMER:
                $this->log(['event' => 'command', 'command' => 'CloseConsumer']);
                unset($this->consumers[$frame->intField(1)]);

                return;

            case PulsarProtocol::COMMAND_PONG:
                $this->log(['event' => 'command', 'command' => 'Pong']);

                return;

            default:
                $this->log(['event' => 'command', 'command' => 'Unknown', 'type' => $type]);
        }
    }

    private function deliver(): void
    {
        foreach (array_keys($this->consumers) as $consumerId) {
            while (($this->consumers[$consumerId]['permits'] ?? 0) > 0) {
                $topic = $this->consumers[$consumerId]['topic'];
                $index = null;

                foreach ($this->topics[$topic] ?? [] as $candidate => $message) {
                    if (!$message['acked'] && $message['delivered_to'] === null) {
                        $index = $candidate;
                        break;
                    }
                }

                if ($index === null) {
                    break;
                }

                $this->topics[$topic][$index]['delivered_to'] = $consumerId;
                $this->topics[$topic][$index]['deliveries']++;
                $this->consumers[$consumerId]['permits']--;
                $message = $this->topics[$topic][$index];

                $this->send(
                    $this->consumers[$consumerId]['client'],
                    PulsarProtocol::COMMAND_MESSAGE,
                    Protobuf::varintField(1, $consumerId)
                    . Protobuf::bytesField(2, $message['id'])
                    . Protobuf::varintField(3, $message['deliveries'] - 1),
                    $message['frame']
                );
            }
        }
    }

    private function acknowledge(int $consumerId, string $label): void
    {
        if (!isset($this->consumers[$consumerId])) {
            return;
        }

        $topic = $this->consumers[$consumerId]['topic'];
        foreach ($this->topics[$topic] ?? [] as $index => $message) {
            if ($message['label'] === $label) {
                $this->topics[$topic][$index]['acked'] = true;
            }
        }
    }

    private function commandBytes(int $type, string $body = '', ?string $payloadFrame = null): string
    {
        $command = Protobuf::varintField(1, $type) . ($body === '' ? '' : Protobuf::bytesField($type, $body));

        return PulsarProtocol::frame($command, $payloadFrame);
    }

    private function send(int $clientId, int $type, string $body = '', ?string $payloadFrame = null): void
    {
        if (!isset($this->clients[$clientId])) {
            return;
        }

        @fwrite($this->clients[$clientId]['socket'], $this->commandBytes($type, $body, $payloadFrame));
    }

    /**
     * @param array<string, mixed> $entry
     */
    private function log(array $entry): void
    {
        if ($this->logFile === null) {
            return;
        }

        file_put_contents($this->logFile, json_encode($entry) . "\n", FILE_APPEND | LOCK_EX);
    }
}

$scenario = $argv[2] ?? 'basic';
$logFile = $argv[1] ?? null;

(new FakePulsarBroker($scenario, $logFile))->run();
