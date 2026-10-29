# Pulsar 驱动（实验）

纯 PHP 实现的 Apache Pulsar 二进制协议客户端（`Erikwang2013\Messaging\Driver\Pulsar`），不依赖 Pulsar 官方 SDK 或扩展。**实验性支持**：仅覆盖最小的生产/消费子集，接口和协议细节可能变化。

## 配置

```php
use Erikwang2013\Messaging\Manager;
use Erikwang2013\Messaging\Driver\Pulsar\PulsarFactory;

$manager = new Manager([
    'default' => 'pulsar',
    'connections' => [
        'pulsar' => [
            'driver' => 'pulsar',
            'service_url' => 'pulsar://127.0.0.1:6650', // 必填
            'topic' => null,                            // 可选，destination 为空时的默认 topic
            'subscription' => 'messaging',              // 默认 messaging
            'consumer_name' => null,                    // 可选，便于在 broker 端识别消费者
            'timeout' => 10.0,                          // 连接、握手、lookup、发布应答超时（秒）
            'tls' => false,                             // true 时强制 TLS，即使 service_url 是 pulsar://
            'tls_context' => [],                        // 传给 PHP stream context 的 SSL 选项
        ],
    ],
]);

// Manager 尚未内置 pulsar，需要先注册工厂，再取连接。
$manager->extend('pulsar', new PulsarFactory());

$conn = $manager->connection('pulsar');
```

`service_url` 只接受单个 broker：`pulsar://host:6650` 或 `pulsar+ssl://host:6651`（默认端口分别为 6650/6651）。不支持逗号分隔的多 broker、URL 路径、用户名密码、query 或 fragment。`tls_context` 可传 `verify_peer`、`cafile`、`local_cert` 等标准 SSL context 选项。

`destination` 即 Pulsar topic。建议使用完整名称 `persistent://tenant/namespace/topic`；短名称依赖 broker 的默认 tenant/namespace 解析。驱动为每个发布/消费 topic 各保持一条独立连接。

## 能力

| 能力 | 支持 | 说明 |
|---|---|---|
| ack | ✓ | 逐条 `CommandAck`（Individual） |
| requeue | ✓ | `reject(true)` 发送 `CommandRedeliverUnacknowledgedMessages`；`reject(false)` 等同 ack（丢弃） |
| delay（发布） | ✓ | `deliverAfterMs` 写入 `MessageMetadata.deliver_at_time`，由 broker 延迟投递 |
| consumer group | ✓ | 每个订阅名一组消费者；驱动只使用 Exclusive 订阅 |
| batch publish | ✗ | 抛 `UnsupportedFeatureException` |
| ordering | ✓ | 单 producer 单 topic 保序；设置 key 时分区键为 `partition_key`。驱动不做分区路由 |
| headers | ✓ | 写入 `MessageMetadata.properties`（字符串键值），key 写入 `partition_key` |
| requiresActiveSubscription | ✗（false） | 首次 `receive()` 时用 `initialPosition=Earliest` 创建订阅，先发布后消费不会丢消息 |

`ReceivedMessage::attempts()` = `redelivery_count + 1`，首次投递为 1。

Header 值会统一转为字符串：`bool` → `'true'`/`'false'`，`null` → `''`，数组/对象 → JSON，其余标量按 PHP 字符串转换。不支持的类型直接抛 `UnsupportedFeatureException`，不会静默丢弃 header 或 key。

## 协议范围

- 帧格式：4 字节大端 `totalSize` + 4 字节大端 `commandSize` + protobuf `BaseCommand`（+ 可选 payload）
- 命令：`Connect`/`Connected`、`LookupTopic`/`LookupResponse`（最多跟随 5 次 redirect）、`Producer`/`ProducerSuccess`、`Send`/`SendReceipt`/`SendError`、`Subscribe`/`Success`、`Flow`、`Message`、`Ack`、`RedeliverUnacknowledgedMessages`、`CloseProducer`/`CloseConsumer`、`Ping`/`Pong`
- 消息帧：magic `0x0e01` + 4 字节 CRC32C + 4 字节 metadata 长度 + `MessageMetadata` + body；校验覆盖 metadata 长度、metadata 和 body
- 协议版本：6；客户端标识 `messaging-php/1.0.0`
- 订阅类型：Exclusive；启动位置：Earliest（见下方偏差说明）
- 每次 `receive()` 最多取一条消息，按需发送 `Flow(1)` 许可

## 限制

- **实验性**：仅实现最小子集，尚未在多种 Pulsar 版本/部署形态上验证。
- **不支持认证**：TLS 传输可用，但不支持 Pulsar token、OAuth 等认证；broker 发起 `AuthChallenge` 时抛 `AuthenticationException`。
- **不支持 batching、压缩、schema、事务、chunking**：入站消息带 compression 或 batch 标记、出站 `publishBatch()` 均抛 `UnsupportedFeatureException`。
- **不支持分区 topic 路由**：驱动把 destination 当作单个 topic。对分区 topic 只能显式指定某个分区（如 `...-partition-0`），不会自动跨分区发布或消费。
- **只使用 Exclusive 订阅**：同一订阅同时只能有一个消费者；第二个消费者会收到 broker 的 `ConsumerBusy`。
- **`reject(true)` 会重投该消费者的全部未确认消息**：Pulsar 的 Exclusive 订阅不支持选择性重投，驱动发送不带 message id 的 redeliver 命令，由 broker 重投所有未 ack 的消息。消费端应把 `reject(true)` 视为「整批未确认消息重新开始」。
- **启动位置是 Earliest**：这是为了让 `requiresActiveSubscription=false` 的契约成立（先发布、后首次 `receive()` 仍能收到）。Pulsar 默认是 `Latest`；只需要新消息的订阅应复用已有订阅名，或由 broker 端管理订阅。
- **发布不自动重试**：`publish()` 遇到 `ConnectionException` 会丢弃生产者连接，下一次调用重连。失败时消息可能已被 broker 持久化，重试可能产生重复，需要业务侧幂等。
- **ack/reject 是单向写**：不等待 broker 确认；连接断开时抛 `ConnectionException`。未 ack 的消息在连接断开后会由 broker 重新投递。
- **Ping 只在读取时回应**：`receive()` 或等待发布应答期间收到 `Ping` 会立即回 `Pong`；两次读取之间没有后台心跳线程。长时间不读取时 broker 可能关闭连接。
- **topic 自动创建依赖 broker 配置**：broker 关闭 auto-topic-creation 且 topic 不存在时，lookup/subscribe 抛 `ProtocolException`（如 `TopicNotFound`、`MetadataError`）。
- 一次 `receive()` 只返回一条消息；批量拉取、多消费者组协调不在范围内。

## 测试

```bash
vendor/bin/phpunit --testsuite unit --filter Pulsar
vendor/bin/phpunit --testsuite integration --filter PulsarTest
```

单元测试覆盖 protobuf 编解码、CRC32C、命令/消息帧、错误映射与工厂参数校验；`tests/Protocol/Pulsar` 使用 `proc_open` 启动的假 broker 覆盖完整连接/发布/消费/重投/超时/畸形帧流程。

集成测试读取 `MESSAGING_PULSAR_URL`（默认 `pulsar://127.0.0.1:6650`），TCP 探测失败时自动 skip。每个测试使用随机 topic 和订阅名，从 Earliest 开始消费；topic 删除与订阅清理依赖 broker 策略。
