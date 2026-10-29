# AWS Kinesis 驱动

驱动名：`kinesis`。纯 PHP 实现 Kinesis Data Streams JSON API 与 SigV4 签名，不依赖 aws/aws-sdk-php。

Kinesis 是分片、可重放的消息流，没有队列式 ack/requeue。本驱动用**游标（checkpoint）**表达消费进度：
`ack()` 记录该分片已消费到的序列号，下次 `receive()` 从该位置之后继续读取。

## 配置

`Manager` 的内置驱动表可能尚未包含 AWS 驱动，使用前注册一次：

```php
use Erikwang2013\Messaging\Manager;
use Erikwang2013\Messaging\Driver\Aws\KinesisFactory;

$manager = new Manager([
    'default' => 'kinesis',
    'connections' => [
        'kinesis' => [
            'driver' => 'kinesis',
            'region' => 'us-east-1',                       // 必填
            'access_key' => 'AKIA...',                     // 可省略，回退到 AWS_ACCESS_KEY_ID
            'secret_key' => '...',                         // 可省略，回退到 AWS_SECRET_ACCESS_KEY
            'session_token' => null,                       // 可选，或 AWS_SESSION_TOKEN
            'endpoint' => null,                            // 可选，LocalStack 如 http://127.0.0.1:4566
            'timeout' => 30.0,                             // HTTP 超时（秒），默认 30.0
            'checkpoint' => null,                          // 可选 callable(string $stream, string $shard, string $sequence)
        ],
    ],
]);
$manager->extend('kinesis', new KinesisFactory());

$conn = $manager->connection('kinesis');
```

`destination` 是流名称（如 `orders`），不是 ARN。流必须已存在且状态为 `ACTIVE`。

## 能力

| 能力 | 支持 | 说明 |
|---|---|---|
| ack | ✗ | 无队列式删除；`ack()` 仅推进内存游标并触发 `checkpoint` 回调 |
| requeue | ✗ | 无重投；`reject()` 抛 `UnsupportedFeatureException` |
| delay（发布） | ✗ | 传入 `delayMs` 抛 `UnsupportedFeatureException` |
| consumer group | ✗ | 使用分片迭代器，无消费者组 |
| batch publish | ✓ | `PutRecords`，单次最多 500 条（驱动会校验） |
| ordering | ✓ | 同一分区键（`PartitionKey`）内有序，按分片保序 |
| headers | ✗ | Kinesis 消息无属性；`headers` 非空时抛 `UnsupportedFeatureException` |

## 行为

- 发送：`PutRecord`，`Data` 为原始 body 的 base64（二进制安全），`PartitionKey` 来自
  `Message::key`；未提供时随机生成 16 字节十六进制串。`PublishResult::id` 为 `SequenceNumber`。
- 批量：`PutRecords`，按输入顺序返回等长结果；部分失败项 `id` 为 `null`，`raw` 为失败条目
  （含 `ErrorCode` / `ErrorMessage`），并记录 warning 日志。
- 接收：`ListShards` 获取分片，首次从 `TRIM_HORIZON`（或已有 checkpoint 的
  `AFTER_SEQUENCE_NUMBER`）取 `GetShardIterator`，`GetRecords` 每批最多 100 条并缓冲。
  一次 `receive()` 返回一条；缓冲区为空时按 `timeoutMs` 轮询。
- `ReceivedMessage::id()` 为序列号，`key()` 为 `PartitionKey`，`attempts()` 恒为 1。
- `ack()` 将 `[流, 分片, 序列号]` 写入内存游标并调用 `checkpoint` 回调；同一分片后续从该序列号之后读取。
- 迭代器过期（`ExpiredIteratorException`）时自动按当前 checkpoint 重建；`NextShardIterator` 为空表示
  分片已关闭，驱动会定期重新 `ListShards` 以发现新分片。
- 超时/连接失败/HTTP 错误映射与其他 AWS 驱动一致。

## 限制

- **checkpoint 只在内存中**：进程退出后丢失，重新从 `TRIM_HORIZON` 读取；需要持久化时通过
  `checkpoint` 回调自行写入 Redis/数据库，并在启动时先 `receive()` 前回填不可行——本驱动不支持
  从外部注入初始 checkpoint，需扩展驱动或在回调侧做去重。这是当前实现最重要的限制。
- 不支持 `SequenceNumberForOrdering`、KPL 聚合、增强型扇出（enhanced fan-out）、跨账户消费。
- 多消费者共享同一流时没有租约/协调，每个实例各自读取全部数据；需要消费者组语义请使用其他驱动。
- `reject()` 不支持；没有死信队列概念。
- 不检查流是否存在或 `ACTIVE`，由 AWS 返回错误；不管理分片扩缩容，重新 `ListShards` 只在无活跃分片时发生。
- `headers` 与 `delay` 不受支持（抛 `UnsupportedFeatureException`）。

## 测试

```bash
vendor/bin/phpunit --testsuite unit --filter Aws
docker compose --profile aws up -d
vendor/bin/phpunit --testsuite integration --filter KinesisTest
```

集成测试会创建单分片流并等待 `ACTIVE`，LocalStack 不可用时自动 skip。
环境变量同 [SQS](./sqs.md#测试)。
