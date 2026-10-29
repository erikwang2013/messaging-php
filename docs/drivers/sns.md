# AWS SNS 驱动

驱动名：`sns`。纯 PHP 实现 SNS Query API 与 SigV4 签名，不依赖 aws/aws-sdk-php。

SNS 是发布/订阅服务，本驱动只负责**向 Topic 发布**；订阅、投递与消费由 SNS 本身及其他驱动（如 SQS）完成。
因此 `receive()` / `publishBatch()` 会抛 `UnsupportedFeatureException`。

## 配置

`Manager` 的内置驱动表可能尚未包含 AWS 驱动，使用前注册一次：

```php
use Erikwang2013\Messaging\Manager;
use Erikwang2013\Messaging\Driver\Aws\SnsFactory;

$manager = new Manager([
    'default' => 'sns',
    'connections' => [
        'sns' => [
            'driver' => 'sns',
            'region' => 'us-east-1',                       // 必填
            'access_key' => 'AKIA...',                     // 可省略，回退到 AWS_ACCESS_KEY_ID
            'secret_key' => '...',                         // 可省略，回退到 AWS_SECRET_ACCESS_KEY
            'session_token' => null,                       // 可选，或 AWS_SESSION_TOKEN
            'endpoint' => null,                            // 可选，LocalStack 如 http://127.0.0.1:4566
            'timeout' => 30.0,                             // HTTP 超时（秒），默认 30.0
        ],
    ],
]);
$manager->extend('sns', new SnsFactory());

$conn = $manager->connection('sns');
```

`destination` 是 Topic ARN，格式 `arn:aws:sns:<region>:<account-id>:<topic-name>`。
驱动不调用 `CreateTopic`，Topic 必须已存在；不存在的 Topic 由 AWS 返回错误并映射为 `RequestException`。

## 能力

| 能力 | 支持 | 说明 |
|---|---|---|
| ack | ✗ | 发布后无确认/重投语义 |
| requeue | ✗ | 同上 |
| delay（发布） | ✗ | SNS 的 `DelaySeconds` 未使用；传入 `delayMs` 抛 `UnsupportedFeatureException` |
| consumer group | ✗ | 无消费概念 |
| batch publish | ✗ | 单次 `Publish`；传入多条抛 `UnsupportedFeatureException` |
| ordering | ✗ | 标准 Topic 不保证顺序；FIFO Topic 未支持 |
| headers | ✓ | `MessageAttributes`，最多 10 个，String / Binary |

`Message::key` 在 SNS 上没有对应语义（SNS 的 `MessageGroupId` 仅用于 FIFO Topic），非空时抛
`UnsupportedFeatureException`，避免静默丢弃。

## 行为

- 使用 Query 协议：`POST /`，`Content-Type: application/x-www-form-urlencoded`，
  `Action=Publish&Version=2010-03-31&TopicArn=...&Message=...`。
- 消息属性编码为 `MessageAttributes.entry.N.Name` / `.Value.DataType` /
  `.Value.StringValue` 或 `.Value.BinaryValue`。
- 响应为 XML；解析 `PublishResponse/PublishResult/MessageId`。缺失 `MessageId` 视为 `ProtocolException`。
- `PublishResult::destination` 原样返回 Topic ARN，`id` 为 `MessageId`，`raw` 保留解析后的响应字段。
- 超时/连接失败/HTTP 错误映射与 SQS 一致（`TimeoutException`、`ConnectionException`、
  `AuthenticationException`、`RequestException`）。

## 限制

- 仅支持 `Publish`。创建/删除 Topic、`Subscribe`/`Unsubscribe`、`SetSubscriptionAttributes` 等管理操作
  不在驱动范围内，需要时可用 `AwsHttpClient::query()`（集成测试即如此）。
- 不支持 FIFO Topic（无 `MessageGroupId` / deduplication id）、不支持 `MessageStructure=json`、
  不支持 SMS 直发。
- `Message` 建议为 UTF-8 文本；SNS 对无效 UTF-8 的消息体不作保证，驱动不做转码。
- 属性名规则与 SQS 相同：1–256 个 `A-Z a-z 0-9 _ . -`，不能以点开头/结尾，不能是 `AWS.` / `Amazon.` 前缀。
- 不校验 256 KiB 消息上限，由 AWS 报错。

## 测试

```bash
vendor/bin/phpunit --testsuite unit --filter Aws
docker compose --profile aws up -d
vendor/bin/phpunit --testsuite integration --filter SnsTest
```

集成测试会创建 Topic，并通过 SQS 队列订阅（`RawMessageDelivery`）验证真实扇出投递，
LocalStack 不可用时自动 skip。环境变量同 [SQS](./sqs.md#测试)。
