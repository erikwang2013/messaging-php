# Kafka 驱动

纯 PHP 实现的 Kafka wire protocol 客户端（`Erikwang2013\Messaging\Driver\Kafka`），不依赖 rdkafka 或官方 SDK。同一套实现通过别名覆盖 Kafka、Redpanda、AutoMQ、Confluent Platform 与 Azure Event Hubs（Kafka 端点）。

## 配置

```php
use Erikwang2013\Messaging\Manager;
use Erikwang2013\Messaging\Driver\Kafka\KafkaFactory;
use Erikwang2013\Messaging\Driver\Kafka\RedpandaFactory;
use Erikwang2013\Messaging\Driver\Kafka\AutoMqFactory;
use Erikwang2013\Messaging\Driver\Kafka\ConfluentFactory;
use Erikwang2013\Messaging\Driver\Kafka\EventHubsFactory;

$manager = new Manager([
    'default' => 'kafka',
    'connections' => [
        'kafka' => [
            'driver' => 'kafka',
            'brokers' => ['127.0.0.1:9092'], // 必填，host:port 数组或逗号分隔字符串
            'group_id' => 'messaging',       // 消费组，默认 messaging
            'client_id' => 'messaging-php',  // 默认 messaging-php
            'timeout' => 10.0,               // 连接与普通请求读超时（秒），默认 10.0
            'tls' => false,                  // 默认 false
            'tls_context' => [],             // 透传给 Socket 的 SSL context 选项
            'auto_offset_reset' => 'latest', // earliest 或 latest，默认 latest
            'acks' => 1,                     // 1 或 -1 (all)，默认 1
            'fetch_max_wait_ms' => 500,      // 单次 Fetch 的最大等待，默认 500
            'session_timeout_ms' => 10000,   // 消费组会话/重平衡超时，默认 10000
            'max_poll_records' => 1,         // 每个分区每次预取并缓存的记录数，默认 1
        ],
    ],
]);

// Manager.php 的内置驱动表尚未包含 Kafka；按当前文件权限，需要显式注册。
$manager->extend('kafka', new KafkaFactory());
$manager->extend('redpanda', new RedpandaFactory());
$manager->extend('automq', new AutoMqFactory());
$manager->extend('confluent', new ConfluentFactory());
$manager->extend('eventhubs', new EventHubsFactory());

$conn = $manager->connection('kafka');
```

`brokers` 中的条目可以省略端口，此时使用驱动别名的默认端口：`eventhubs` 为 9093，其余（`kafka`、`redpanda`、`automq`、`confluent`）为 9092。支持 `host:port`、`[ipv6]:port` 和裸主机名。

`destination` 即 Kafka topic。发布端按需连接各分区 leader；消费端使用经典 consumer group 协议，一个驱动实例对应一个消费组会话。

## 能力

| 能力 | 支持 | 说明 |
|---|---|---|
| ack | ✓ | 提交 `offset + 1` 到消费组 |
| requeue | ✓ | `reject(true)` 将本地读取位置回退到该消息 offset；`reject(false)` 等同 ack（提交并跳过） |
| delay（发布） | ✗ | 带 `delayMs` 抛 `UnsupportedFeatureException` |
| consumer group | ✓ | FindCoordinator / JoinGroup / SyncGroup / Heartbeat / LeaveGroup、range assignor |
| batch publish | ✓ | 每批最多受 broker/消息大小限制，按分区合并为一个 record batch |
| ordering | ✓ | 同一分区内有序；跨分区不保证 |
| headers | ✓ | header 值统一转为字符串；`null` 保留为 null |
| requiresActiveSubscription | ✗ | 首次 `receive()` 时自动加入消费组 |

`ReceivedMessage::raw()` 返回 `topic`、`partition`、`offset`、`timestamp`；`id()` 形如 `topic-partition-offset`，发布与消费两侧的 id 一致。

## 协议与版本范围

驱动在建立每条连接时先发送 ApiVersions v0；若 broker 返回 `UNSUPPORTED_VERSION`、无法解析或断开连接，则改用 flexible v3 重试。协商失败或 broker 不支持下列版本时抛 `UnsupportedFeatureException`，错误信息包含 broker 地址、API key、broker 支持区间和所需版本。

| API | 版本 |
|---|---|
| ApiVersions (18) | 请求 v0，失败时回退 v3 |
| Metadata (3) | 4（允许自动建 topic） |
| Produce (0) | 3 |
| Fetch (1) | 4 |
| ListOffsets (2) | 1 |
| FindCoordinator (10) | 1 |
| JoinGroup (11) | 2 |
| SyncGroup (14) | 2 |
| Heartbeat (12) | 1 |
| LeaveGroup (13) | 2 |
| OffsetCommit (8) | 2 |
| OffsetFetch (9) | 2 |

记录格式为 record batch magic 2（base offset、leader epoch、CRC32C、attributes、时间戳、producer id/epoch/sequence、varint 编码的 key/value/headers）。CRC32C 优先使用 `hash('crc32c')`（PHP 8.1+），否则使用纯 PHP 查表实现。

压缩：消费端支持 gzip（ext-zlib）；其他压缩 codec 会抛 `UnsupportedFeatureException`。发布端当前发送未压缩 batch（配置项里没有请求压缩开关）。

## 语义

- **分区选择**：带 key 时使用 Kafka 默认的 murmur2 hash（seed `0x9747b28c`）对分区数取模；无 key 时在分区列表上轮询。
- **发布确认**：`acks` 只接受 1 或 -1。不自动重试；失败直接抛异常，重试可能产生重复消息。
- **批量发布**：按分区合并消息，每个 leader 一次 Produce；返回结果顺序与输入一致，id 为该分区 batch 的 `base_offset + 批内序号`。
- **消费**：每次 `receive()` 返回一条消息，内部按分区缓冲最多 `max_poll_records` 条。`timeoutMs` 包含加入消费组的时间；超时返回 `null`。
- **提交**：ack 时向 coordinator 提交 `offset + 1`。`reject(true)` 清空该分区缓冲并把读取位置回退到当前 offset，下一次 `receive()` 会重新拉到它；`reject(false)` 提交并跳过。
- **自动重置**：没有已提交 offset（或 broker 返回 `OFFSET_OUT_OF_RANGE`）时，按 `auto_offset_reset` 用 ListOffsets 取 earliest/latest。
- **重平衡**：Heartbeat 返回 `REBALANCE_IN_PROGRESS` / `ILLEGAL_GENERATION` / `UNKNOWN_MEMBER_ID`，或 coordinator 错误时，驱动重新 FindCoordinator、JoinGroup、SyncGroup，并按 range assignor 重新分配分区。被撤销分区的本地缓冲会丢弃；未提交的消息可能由新 owner 重新投递，因此语义是 at-least-once。
- **心跳**：按 `session_timeout_ms / 3` 的间隔在 `receive()` 循环中发送，单次 Fetch 等待也被限制在该间隔内，避免长时间 fetch 导致会话过期。
- **关闭**：`close()` 会尽力发送 LeaveGroup，并关闭所有 broker 连接。

## 别名

| 别名 | 默认端口 | 工厂类 |
|---|---|---|
| `kafka` | 9092 | `KafkaFactory` |
| `redpanda` | 9092 | `RedpandaFactory` |
| `automq` | 9092 | `AutoMqFactory` |
| `confluent` | 9092 | `ConfluentFactory` |
| `eventhubs` | 9093 | `EventHubsFactory` |

所有别名使用完全相同的 wire protocol；差异仅在默认端口和文档说明。工厂也会读取连接配置里的 `driver` 名称：如果它是已知别名，则优先于工厂自身的默认别名。

Azure Event Hubs 的 Kafka 端点默认要求 SASL/PLAIN（连接字符串或 Entra ID token）。**本驱动 v1 不实现 SASL**，因此无法连接 Event Hubs 公共端点；仅当端点关闭 SASL（例如私有化/自建网关）时可用。使用 `eventhubs` 别名时驱动会通过 PSR-3 logger 输出一条 warning，不会静默假装支持。Confluent Cloud 等托管服务通常同样需要 SASL/SSL，v1 不可用。

## 限制

- 不支持 SASL（PLAIN/SCRAM/OAUTHBEARER）和 SASL 握手；只支持明文和 TLS（`tls` + `tls_context`）。
- 不支持事务、幂等 producer、exactly-once；producer id 固定为 -1，不发送 InitProducerId。
- 不支持压缩发布；消费只支持 gzip，snappy/lz4/zstd 抛 `UnsupportedFeatureException`。
- 不支持 CreateTopics/DeleteTopics 等 admin API；依赖 broker 的 `auto.create.topics.enable=true` 或预先创建 topic。
- Tombstone（value 为 null）在消费侧会变成空字符串 `''`，无法区分 null 与空 body；发布侧 `Message::$body` 是 string，也不能发送 tombstone。
- 只实现了经典 consumer group 的 `range` assignor；不支持 cooperative-sticky、static membership（`group.instance.id`）、KIP-848 新消费组协议。与使用其他 assignor 的客户端混用同一个 group 可能反复重平衡或加入失败。
- 只支持按 earliest/latest 重置 offset，不支持按时间戳查询 offset，也不支持 `auto_offset_reset=none`。
- 发布失败不自动重试；调用方重试可能产生重复消息。`acks=1` 时 leader 故障可能丢消息。
- `receive()` 的超时包含首次 JoinGroup 的等待；Kafka 默认 `group.initial.rebalance.delay.ms=3000`，短超时的首次调用可能返回 `null` 但已完成入组。集成测试因此先做一次热身调用。
- 每个驱动实例只应有一个消费循环；PHP 同步模型下不是线程安全的。

## 测试

```bash
vendor/bin/phpunit --testsuite unit --filter Kafka

# 需要 Kafka 兼容 broker；连接失败会自动 skip
MESSAGING_KAFKA_BROKERS=127.0.0.1:9092 \
    vendor/bin/phpunit --testsuite integration --filter KafkaTest
```

集成测试使用唯一 topic，默认连接 `127.0.0.1:9092`，并设置 `auto_offset_reset=earliest` 以覆盖「先发布、后加入消费组」的共享契约测试。topic 需要允许自动创建，或由 broker 预先配置。协议测试使用 `tests/Protocol/Kafka/fixtures/fake_kafka_broker.php` 起的本地假 broker，覆盖 ApiVersions v0/v3 回退、Metadata、Produce、Fetch、消费组加入/同步/心跳、OffsetFetch/Commit、超时、损坏帧和版本不支持。
