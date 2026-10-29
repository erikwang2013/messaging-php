<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Aws;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\Capabilities;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\PublishResult;
use Erikwang2013\Messaging\Contract\ReceivedMessage;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use GuzzleHttp\ClientInterface;
use Psr\Log\LoggerInterface;

/**
 * AWS SNS driver (publish-only) using the Query protocol and SigV4.
 */
final class SnsDriver implements DriverInterface
{
    private AwsHttpClient $client;

    public function __construct(ConnectionConfig $config, ?LoggerInterface $logger = null, ?ClientInterface $http = null)
    {
        $this->client = new AwsHttpClient(AwsConfig::fromConnectionConfig($config), $logger, $http);
    }

    public function publish(string $destination, Message $message, array $options = []): PublishResult
    {
        if (preg_match('~^arn:aws[a-z-]*:sns:[a-z0-9-]+:\d{12}:.+$~', $destination) !== 1) {
            throw new \InvalidArgumentException(sprintf('Invalid SNS topic ARN [%s].', $destination));
        }

        if ($message->key !== null) {
            throw new UnsupportedFeatureException('SNS does not support message keys.');
        }

        if ($message->delayMs !== null) {
            throw new UnsupportedFeatureException('SNS does not support delayed delivery.');
        }

        $params = [
            'Action' => 'Publish',
            'Version' => '2010-03-31',
            'TopicArn' => $destination,
            'Message' => $message->body,
        ];

        $index = 1;
        foreach (MessageAttributes::encode($message->headers) as $name => $attribute) {
            $prefix = 'MessageAttributes.entry.' . $index++;
            $params[$prefix . '.Name'] = $name;
            $params[$prefix . '.Value.DataType'] = $attribute['DataType'];
            if (isset($attribute['StringValue'])) {
                $params[$prefix . '.Value.StringValue'] = $attribute['StringValue'];
            }
            if (isset($attribute['BinaryValue'])) {
                $params[$prefix . '.Value.BinaryValue'] = $attribute['BinaryValue'];
            }
        }

        $xml = $this->client->query('sns', $this->client->endpoint('sns'), $params);
        $id = self::tag($xml, 'MessageId');
        if ($id === null) {
            throw new ProtocolException('SNS Publish response is missing MessageId.');
        }

        return new PublishResult($destination, $id, [
            'MessageId' => $id,
            'RequestId' => self::tag($xml, 'RequestId'),
        ]);
    }

    public function publishBatch(string $destination, array $messages, array $options = []): array
    {
        throw new UnsupportedFeatureException('SNS does not support batch publishing.');
    }

    public function receive(string $destination, ReceiveOptions $options): ?ReceivedMessage
    {
        throw new UnsupportedFeatureException('SNS is publish-only and cannot receive messages.');
    }

    public function capabilities(): Capabilities
    {
        return new Capabilities(
            ack: false,
            requeue: false,
            delay: false,
            consumerGroups: false,
            batchPublish: false,
            ordering: false,
            headers: true,
        );
    }

    public function close(): void
    {
    }

    private static function tag(string $xml, string $tag): ?string
    {
        $pattern = '~<' . preg_quote($tag, '~') . '>(.*?)</' . preg_quote($tag, '~') . '>~s';
        if (preg_match($pattern, $xml, $matches) !== 1) {
            return null;
        }

        return html_entity_decode($matches[1], ENT_QUOTES | ENT_XML1, 'UTF-8');
    }
}
