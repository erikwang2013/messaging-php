# AWS SQS 驱动

驱动名：`sqs`。纯 PHP 实现 AWS SQS JSON 协议与 SigV4 签名，不依赖 aws/aws-sdk-php。

## 配置

`Manager` 的内置驱动表可能尚未包含 AWS 驱动，使用前注册一次：

```php
use Erikwang2013\Messaging\Manager;
use Erikwang2013\Messaging\Driver\Aws\SqsFactory;

$manager = new Manager([
    'default' => 'sqs',
    'connections' => [
        'sqs' => [
            'driver' => 'sqs',
            'region' => 'us-east-1',                       // 必填
            'access_key' => 'AKIA...',                     // 可省略，回退到 AWS_ACCESS_KEY_ID
            'secret_key' => '...',                         // 可省略，回退到 AWS_SECRET_ACCESS_KEY
            'session_token' => null,                       // 可选，或 AWS_SESSION_TOKEN
            'endpoint' => null,                            // 可选，LocalStack 如 http://127.0.0.1:4566
            'timeout' => 30.0,                             // HTTP 超时（秒），默认 30.0
            'visibility_timeout' => 30,                    // 接收后的可见性超时（秒），默认 30
        ],
    ],
]);
$manager->extend('sqs', new SqsFactory());

$conn = $manager->connection('sqs');
```

`destination` 可以是完整队列 URL（`https://sqs.us-east-1.amazonaws.com/123456789012/orders`），
也可以是队列名（`orders`）。队列名首次使用时调用 `GetQueueUrl` 并缓存；完整 URL 直接使用、不会探测。
FIFO 队列 URL 以 `.fifo` 结尾。

## 能力

| 能力 | 支持 | 说明 |
|---|---|---|
| ack | ✓ | `DeleteMessage` |
| requeue | ✓ | `reject(true)` 执行 `ChangeMessageVisibility` 0；`reject(false)` 等同 ack（删除） |
| delay（发布） | ✓ | `delayMs` 向上取整为 `DelaySeconds`（0–900）；FIFO 队列不支持，抛 `UnsupportedFeatureException` |
| consumer group | ✗ | SQS 无消费者组；多消费者天然竞争消费 |
| batch publish | ✓ | `SendMessageBatch`，单次最多 10 条 |
| ordering | ✗ | 标准队列不保证顺序；FIFO 队列仅在提供 `Message::key` 时按 `MessageGroupId` 有序 |
| headers | ✓ | `MessageAttributes`，最多 10 个；标量走 String，二进制或结构走 Binary |

`ReceivedMessage::attempts()` 来自 `ApproximateReceiveCount`（首次投递为 1）。
`Message::key` 映射为 `MessageGroupId`，仅在 FIFO 队列上可用；收到 FIFO 消息时 `key()` 返回 `MessageGroupId`。

## 行为

- `receive()` 的长轮询 `WaitTimeSeconds` 由 `timeoutMs` 推导（0–20 秒）；`timeoutMs = 0` 表示无限阻塞。
  `timeoutMs` 小于 1 秒时使用短轮询，未取到消息则等到超时后返回 `null`。
- 每次接收请求 `MaxNumberOfMessages = 1`，并请求 `AttributeNames = All` 与 `MessageAttributeNames = All`。
- `publishBatch()` 返回与输入等长的 `PublishResult` 列表；部分失败时对应项 `id` 为 `null`，
  `raw` 为失败条目（含 `Code` / `Message`），并记录 warning 日志。
- HTTP 403/401 映射为 `AuthenticationException`；其余错误状态映射为带 `statusCode()` / `responseBody()` 的
  `RequestException`；连接失败与超时分别映射为 `ConnectionException` / `TimeoutException`。

## 限制

- **消息体必须是合法 UTF-8**：SQS 的 `MessageBody` 是字符串，二进制负载不支持。非 UTF-8 body 或空 body
  在发送前抛 `InvalidArgumentException`，不会静默转码。
- **FIFO 去重**：驱动只设置 `MessageGroupId`（来自 `key`），不设置 `MessageDeduplicationId`。
  FIFO 队列需启用 content-based deduplication，否则 AWS 会拒绝请求。
- **FIFO 不支持逐条延迟**：FIFO 队列上带 `delayMs` 抛 `UnsupportedFeatureException`。
- 不校验 256 KiB 消息上限、不管理死信队列 / 队列属性 / 标签；这些由 AWS 报错并映射为 `RequestException`。
- 不支持 SQS 的 `PurgeQueue`、`ListQueues` 等管理操作；集成测试通过内部 `AwsHttpClient` 调用管理 API。
- 消息属性名需为 1–256 个 `A-Z a-z 0-9 _ . -`，不能以点开头/结尾，且不能使用 `AWS.` / `Amazon.` 前缀。

## 测试

```bash
vendor/bin/phpunit --testsuite unit --filter Aws
docker compose --profile aws up -d
vendor/bin/phpunit --testsuite integration --filter SqsTest
```

集成测试读取 `MESSAGING_AWS_ENDPOINT`（默认 `http://127.0.0.1:4566`）、`MESSAGING_AWS_REGION`
（默认 `us-east-1`）、`MESSAGING_AWS_ACCESS_KEY` / `MESSAGING_AWS_SECRET_KEY`（默认 `test`），
LocalStack 不可用时自动 skip。
