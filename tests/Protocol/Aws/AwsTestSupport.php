<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Protocol\Aws;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Driver\Aws\AwsConfig;
use Erikwang2013\Messaging\Driver\Aws\AwsHttpClient;
use Erikwang2013\Messaging\Driver\Aws\KinesisFactory;
use Erikwang2013\Messaging\Driver\Aws\SnsFactory;
use Erikwang2013\Messaging\Driver\Aws\SqsFactory;

/**
 * AWS-compatible endpoint (Moto) helpers shared by the AWS integration tests.
 */
final class AwsTestSupport
{
    /**
     * @return array<string, mixed>
     */
    public static function options(): array
    {
        return [
            'region' => getenv('MESSAGING_AWS_REGION') ?: 'us-east-1',
            'access_key' => getenv('MESSAGING_AWS_ACCESS_KEY') ?: 'test',
            'secret_key' => getenv('MESSAGING_AWS_SECRET_KEY') ?: 'test',
            'endpoint' => getenv('MESSAGING_AWS_ENDPOINT') ?: 'http://127.0.0.1:5000',
            'timeout' => 5.0,
        ];
    }

    /**
     * @param array<string, mixed> $options
     */
    public static function driver(string $name, array $options = []): DriverInterface
    {
        $factory = match ($name) {
            'sqs' => new SqsFactory(),
            'sns' => new SnsFactory(),
            'kinesis' => new KinesisFactory(),
        };

        return $factory->create(ConnectionConfig::fromArray($name, $options + self::options() + ['driver' => $name]));
    }

    public static function client(): AwsHttpClient
    {
        return new AwsHttpClient(AwsConfig::fromConnectionConfig(
            ConnectionConfig::fromArray('aws-admin', self::options() + ['driver' => 'sqs']),
        ));
    }

    public static function createQueue(string $name, bool $fifo = false): string
    {
        $payload = ['QueueName' => $name];
        if ($fifo) {
            $payload['Attributes'] = ['FifoQueue' => 'true', 'ContentBasedDeduplication' => 'true'];
        }

        $data = self::client()->json('sqs', 'AmazonSQS.CreateQueue', self::client()->endpoint('sqs'), $payload);
        $url = $data['QueueUrl'] ?? null;
        if (!is_string($url) || $url === '') {
            throw new \RuntimeException('SQS CreateQueue response is missing QueueUrl.');
        }

        return $url;
    }

    public static function deleteQueue(string $queueUrl): void
    {
        self::client()->json('sqs', 'AmazonSQS.DeleteQueue', self::client()->endpoint('sqs'), ['QueueUrl' => $queueUrl]);
    }

    public static function queueArn(string $queueUrl): string
    {
        $data = self::client()->json('sqs', 'AmazonSQS.GetQueueAttributes', self::client()->endpoint('sqs'), [
            'QueueUrl' => $queueUrl,
            'AttributeNames' => ['QueueArn'],
        ]);

        $attributes = $data['Attributes'] ?? null;
        $arn = is_array($attributes) ? ($attributes['QueueArn'] ?? null) : null;
        if (!is_string($arn) || $arn === '') {
            throw new \RuntimeException('SQS GetQueueAttributes response is missing QueueArn.');
        }

        return $arn;
    }

    public static function createTopic(string $name): string
    {
        $xml = self::client()->query('sns', self::client()->endpoint('sns'), [
            'Action' => 'CreateTopic',
            'Version' => '2010-03-31',
            'Name' => $name,
        ]);

        $arn = self::xmlTag($xml, 'TopicArn');
        if ($arn === null) {
            throw new \RuntimeException('SNS CreateTopic response is missing TopicArn.');
        }

        return $arn;
    }

    public static function deleteTopic(string $topicArn): void
    {
        self::client()->query('sns', self::client()->endpoint('sns'), [
            'Action' => 'DeleteTopic',
            'Version' => '2010-03-31',
            'TopicArn' => $topicArn,
        ]);
    }

    public static function subscribeQueue(string $topicArn, string $queueArn): void
    {
        self::client()->query('sns', self::client()->endpoint('sns'), [
            'Action' => 'Subscribe',
            'Version' => '2010-03-31',
            'TopicArn' => $topicArn,
            'Protocol' => 'sqs',
            'Endpoint' => $queueArn,
            'Attributes.entry.1.key' => 'RawMessageDelivery',
            'Attributes.entry.1.value' => 'true',
        ]);
    }

    public static function createStream(string $name): void
    {
        self::client()->json('kinesis', 'Kinesis_20131202.CreateStream', self::client()->endpoint('kinesis'), [
            'StreamName' => $name,
            'ShardCount' => 1,
        ], null, '1.1');
    }

    public static function waitForStream(string $name): void
    {
        for ($attempt = 0; $attempt < 60; $attempt++) {
            $data = self::client()->json('kinesis', 'Kinesis_20131202.DescribeStreamSummary', self::client()->endpoint('kinesis'), [
                'StreamName' => $name,
            ], null, '1.1');

            $summary = $data['StreamDescriptionSummary'] ?? null;
            $status = is_array($summary) ? ($summary['StreamStatus'] ?? null) : null;
            if ($status === 'ACTIVE') {
                return;
            }

            usleep(200_000);
        }

        throw new \RuntimeException(sprintf('Kinesis stream [%s] did not become ACTIVE.', $name));
    }

    public static function deleteStream(string $name): void
    {
        self::client()->json('kinesis', 'Kinesis_20131202.DeleteStream', self::client()->endpoint('kinesis'), [
            'StreamName' => $name,
            'EnforceConsumerDeletion' => true,
        ], null, '1.1');
    }

    private static function xmlTag(string $xml, string $tag): ?string
    {
        $pattern = '~<' . preg_quote($tag, '~') . '>(.*?)</' . preg_quote($tag, '~') . '>~s';
        if (preg_match($pattern, $xml, $matches) !== 1) {
            return null;
        }

        return html_entity_decode($matches[1], ENT_QUOTES | ENT_XML1, 'UTF-8');
    }
}
