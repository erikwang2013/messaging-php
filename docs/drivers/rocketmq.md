# RocketMQ 驱动（实验性）

纯 PHP 实现的 Apache RocketMQ 4.x remoting 协议客户端（`Erikwang2013\Messaging\Driver\RocketMq`），不依赖官方 SDK 或扩展。支持 4.x broker，以及开启 remoting 兼容端口的 5.x broker；不使用 gRPC。

**实验性**：协议覆盖发布、拉取消费、ack/requeue、固定延迟和路由查询，但不包含事务、ACL、消息轨迹、SQL92 过滤和阿里云 ONS 认证。生产使用前请自行压测。

## 配置

```php
use Erikwang2013\Messaging\Manager;
use Erikwang2013\Messaging\Driver\RocketMq\RocketMqFactory;

$manager = new Manager([
    'default' => 'rocketmq',
    'connections' => [
        'rocketmq' => [
            'driver' => 'rocketmq',
            'nameserver' => '127.0.0.1:9876', // 必填，host:port；省略端口时用 9876
            'group' => 'messaging',           // 消费组/生产者组名，默认 messaging
            'topic' => null,                  // 默认 topic；destination 为空字符串时使用
            'timeout' => 10.0,                // 连接与单次请求读超时（秒），默认 10.0
            'heartbeat_interval' => 30.0,     // 重复发送 HEART_BEAT 的间隔（秒），默认 30，最小 1
            'consume_from' => 'last',         // last 或 first，默认 last
            'pull_batch_size' => 1,           // 单次 PULL_MESSAGE 的 maxMsgNums，默认 1
        ],
    ],
]);

// Manager.php 的内置驱动表尚未包含 RocketMQ；需要显式注册。
$manager->extend('rocketmq', new RocketMqFactory());

$conn = $manager->connection('rocketmq');
```

`nameserver` 支持 `host`、`host:port`、`[ipv6]` 和 `[ipv6]:port`。`group`、`topic` 与 destination 必须符合 RocketMQ 名称规则（`%`、字母、数字、`_`、`-`，最长 127）。topic 和消费组都需要在 broker 上允许自动创建（默认开启），或提前创建。

`destination` 即 RocketMQ topic。

## 能力

| 能力 | 支持 | 说明 |
|---|---|---|
| ack | ✓ | `UPDATE_CONSUMER_OFFSET` 提交 `queueOffset + 1` |
| requeue | ✓ | `reject(true)` 通过 `CONSUMER_SEND_MSG_BACK` 投递到 `%RETRY%group`，延迟 1 秒后重新消费；`reject(false)` 提交并丢弃 |
| delay（发布） | ✓ | `delayMs` 映射到 broker 默认延迟级别（1s…2h）；超过 2 小时抛 `InvalidArgumentException` |
| consumer group | ✓ | 每个驱动实例一个消费组；HeartBeat 注册并创建 retry topic |
| batch publish | ✗ | `publishBatch()` 抛 `UnsupportedFeatureException` |
| ordering | ✓ | 同一 queue 内按 queueOffset 有序；跨 queue 不保证 |
| headers | ✓ | 标量 header 转字符串；保留 key（`KEYS`）和延迟 |
| requiresActiveSubscription | ✗ | 首次 `receive()` 时按需拉取，无需预订阅 |

`ReceivedMessage::id()` 是消息属性 `UNIQ_KEY`，发布时由驱动生成并随响应返回，因此发布与消费两侧 id 一致。`key()` 来自 `KEYS`。`attempts()` 为 broker 的 `reconsumeTimes + 1`，重试消息为 2 起。`raw()` 返回解析后的存储消息数组（topic、queueId、queueOffset、physicalOffset、reconsumeTimes、properties 等）。

## 协议与版本范围

remoting 帧格式：`totalLen(4) + headerLen(4) + JSON header + body`。`totalLen` 不含自身的 4 字节；`headerLen` 低 24 位为长度，高 8 位为序列化类型（只支持 0，即 JSON）。header 字段为 `code`、`language`（`PHP`）、`version`（`407`，即 4.9.7 客户端版本）、`opaque`、`flag`、`remark`、`extFields`。响应通过 `flag & 1` 区分，`opaque` 做请求关联。

| 请求 | code | 用途 |
|---|---|---|
| SEND_MESSAGE | 10 | 发布；extFields 含 producerGroup、topic、queueId、sysFlag、bornTimestamp、flag、properties、defaultTopic、defaultTopicQueueNums |
| PULL_MESSAGE | 11 | 拉取；consumerGroup、topic、queueId、queueOffset、maxMsgNums、sysFlag（只带 SUBSCRIPTION 位）、subscription |
| QUERY_CONSUMER_OFFSET | 14 | 每个队列首次拉取前查询 broker 已提交位点；未找到时按 `consume_from` 回退 |
| UPDATE_CONSUMER_OFFSET | 15 | ack/reject 提交位点 |
| GET_MAX_OFFSET / GET_MIN_OFFSET | 30 / 31 | 首次拉取时按 `consume_from` 确定起始位点 |
| HEART_BEAT | 34 | 注册消费组与订阅、触发 `%RETRY%group` topic 创建；按间隔重复发送 |
| CONSUMER_SEND_MSG_BACK | 36 | requeue；携带物理 offset、group、delayLevel、originMsgId、originTopic |
| GET_ROUTEINFO_BY_TOPIC | 105 | 从 nameserver 查询 TopicRouteData |

响应码处理：`SUCCESS(0)` 正常；`PULL_NOT_FOUND(19)`、`PULL_RETRY_IMMEDIATELY(20)` 视为本次无消息；`PULL_OFFSET_MOVED(21)` 采用响应的 `nextBeginOffset`；`NO_PERMISSION(16)` 抛 `AuthenticationException`；其余非零码抛 `RequestException`，`statusCode()` 为 broker 响应码，`responseBody()` 为 remark。

发布流程：先查 topic 路由；topic 尚不存在时回退到 `TBW102` 默认 topic 的路由发送，由 broker 自动建 topic，随后刷新路由。带 key 时按 `crc32(key)` 选择写队列，否则轮询。成功响应的 `msgId`、`queueId`、`queueOffset` 放在 `PublishResult::$raw`。

消费流程：短轮询（每轮间隔 20–50ms），按 queue 轮转 PULL_MESSAGE；解析 broker 返回的连续存储消息记录（magic `0xDAA320A7` / `0xDAA320AB`，含 IPv6 host 标志位）。不发送 long-polling 的 suspend 标志。每个队列第一次拉取前先用 QUERY_CONSUMER_OFFSET 查询 broker 已提交位点，没有已提交位点时才按 `consume_from` 取 min/max；retry 队列在无已提交位点时始终从最小位点开始，避免漏掉刚 requeue 的消息。同时消费 `%RETRY%group`，按消息属性 `RETRY_TOPIC` 归还原 destination。压缩消息（compressed flag）抛 `UnsupportedFeatureException`。

## 语义

- **发布**：同步发送，一次一条。失败直接抛异常，不自动重试；重试可能产生重复消息。
- **消费位点**：每个队列首次拉取前调用 `QUERY_CONSUMER_OFFSET`；broker 返回已提交位点（>= 0）时从该位点继续，否则按 `consume_from` 取 min/max（retry 队列取 min）。ack/reject 时通过 `UPDATE_CONSUMER_OFFSET` 提交。进程重启后会从 broker 已提交位点恢复，崩溃前已拉取但未 ack 的消息会重新投递。
- **心跳**：按 `heartbeat_interval`（默认 30 秒，最小 1 秒）对每个 broker 地址重复发送 `HEART_BEAT`，`subscriptionDataSet` 携带当前 destination 的订阅（`subString` 为 `*`），避免空闲消费者被 broker 移除；连接断开后下次拉取会重新注册。
- **ack / reject**：`ack()` 与 `reject(false)` 提交 `queueOffset + 1`；提交是单调的，旧位点不会覆盖新位点。`reject(true)` 先 send-back，再提交原队列位点；send-back 失败会回退本地拉取位点并抛出异常，消息不会被静默丢弃。
- **重试**：broker 固定把 send-back 消息投递到 `%RETRY%group`，驱动自动消费并还原 topic，`attempts()` 递增。重试延迟固定为 broker 延迟级别 1（1 秒），不读取消息头里的 `DELAY` 级别。
- **延迟发布**：`delayMs` 选择不小于请求值的最小 broker 延迟级别：1s、5s、10s、30s、1m、2m…10m、20m、30m、1h、2h 共 18 级。请求超过 2 小时会抛异常。
- **headers**：`Message::$headers` 的标量值按字符串写入消息属性；接收时除 RocketMQ 系统属性外的属性作为 headers 返回（值均为字符串）。`KEYS`、`UNIQ_KEY`、`WAIT`、`DELAY`、`RETRY_TOPIC` 等系统属性名不能作为自定义 header，否则抛 `InvalidArgumentException`。
- **并发**：同步实现，每个驱动实例只应有一个消费循环；PHP 同步模型下不是线程安全的。

## 限制

- 实验性协议实现；未在全部 4.x/5.x 版本矩阵上验证，5.x 需要 broker 开启 remoting 端口。
- 不支持事务消息、事务回查、消息轨迹、ACL/鉴权、阿里云 ONS/SASL 认证。
- 不支持 SQL92 或 TAG 过滤：PULL_MESSAGE 使用空订阅（等同 `*`），所有消息都会返回。
- 不支持批量发布、批量拉取之外的高级流控/消费重平衡；`pull_batch_size > 1` 时多余消息缓存在进程内。
- 不支持压缩消息体；接收压缩消息会抛 `UnsupportedFeatureException`。
- 不支持 broker 端 long polling（suspend），空闲时按 20–50ms 间隔短轮询，队列多时请求量较大。
- 心跳只在拉取前按间隔发送；驱动实例完全没有 `receive()` 调用时不会在后台发送心跳。
- 只有 master broker 参与读写；不支持从 slave 拉取、broker 切换时的自动重选和 `suggestWhichBrokerId` 重定向。
- 延迟只支持 broker 默认的 18 个级别，不支持自定义 `messageDelayLevel` 的时间表。
- 不检查 `defaultTopicQueueNums` 与 broker 配置的差异；依赖 `autoCreateTopicEnable=true` 或预建 topic。
- 生产端不发送 HeartBeat、不注册 producer group；依赖 broker 的宽松校验。

## 测试

```bash
vendor/bin/phpunit --testsuite unit --filter RocketMq

# 需要 RocketMQ nameserver；不可达时自动 skip
MESSAGING_ROCKETMQ_NAMESERVER=127.0.0.1:9876 \
MESSAGING_ROCKETMQ_GROUP=messaging-contract \
    vendor/bin/phpunit --testsuite integration --filter RocketMqTest
```

集成测试默认连接 `127.0.0.1:9876`，使用唯一 topic 和（默认随机）唯一消费组，`consume_from=first` 以覆盖“先发布、后消费”的共享契约测试；requeue 测试把第二次 `receive()` 超时放宽到 10 秒等待 broker 的 1 秒重试延迟。broker 需允许自动创建 topic 和消费组。

协议测试使用 `tests/Protocol/RocketMq/fixtures/fake_rocketmq.php` 启动本地假 nameserver + broker，覆盖路由查询、发送、拉取、位点提交、send-back、发布超时、拉取超时和损坏帧。
