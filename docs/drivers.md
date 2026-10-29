# 驱动支持矩阵

20 个产品名由 13 套协议栈承载；P1–P5 已全部实现（v1.8.0），其中 Pulsar、RocketMQ 为实验性支持。状态列同时标注真实 broker 的验证范围。

| 驱动名 | 覆盖系统 | 协议 | 支持级别 | 状态与验证 |
|---|---|---|---|---|
| `rabbitmq` | RabbitMQ | AMQP 0-9-1 | 一等 | 已实现（P3；默认交换机，无 publisher confirm；真实 RabbitMQ 集成测试通过） |
| `kafka` `redpanda` `automq` `confluent` `eventhubs` | Kafka、Redpanda、AutoMQ、Confluent Platform、Azure Event Hubs（Kafka 端点） | Kafka wire protocol | 一等（Event Hubs 复用 Kafka 端点） | 已实现（P4；无 SASL，Event Hubs 需 SASL 才可连；Kafka/Redpanda 集成测试通过，AutoMQ/Confluent/Event Hubs 未单独跑真实服务） |
| `activemq` `artemis` | ActiveMQ Classic、ActiveMQ Artemis | STOMP 1.2 | 一等 | 已实现（P1；真实 broker 集成测试通过） |
| `pulsar` | Apache Pulsar | Pulsar 二进制协议 | 实验 | 已实现（P5；真实 Pulsar 3.3.2 验证；协议 v11、Shared 订阅） |
| `rocketmq` | Apache RocketMQ 4.x/5.x | RocketMQ remoting | 实验 | 已实现（P5；协议/模拟测试，未跑真实 broker；周期心跳 + broker 位点恢复） |
| `nats` | NATS Core + JetStream | NATS 文本协议 | 一等（Core）/ 实验（JetStream） | Core 已实现（P1；真实 NATS 集成测试通过）；JetStream 未实现 |
| `redis` | Redis Streams | RESP | 一等 | 已实现（P1；真实 Redis 集成测试通过） |
| `sqs` | AWS SQS | AWS JSON API + SigV4 | 一等 | 已实现（P2；Moto 模拟的 AWS 集成测试通过，未跑真实 AWS） |
| `sns` | AWS SNS | AWS Query API + SigV4 | 仅发布 | 已实现（P2；仅发布；Moto 模拟） |
| `kinesis` | AWS Kinesis | AWS JSON API + SigV4 | 一等（简化 checkpoint） | 已实现（P2；Moto 模拟） |
| `gcp-pubsub` | Google Cloud Pub/Sub | REST + 服务账号 JWT | 一等 | 已实现（P2；Pub/Sub emulator 集成测试通过） |
| `azure-service-bus` | Azure Service Bus | REST + SAS | 一等 | 已实现（P2；内存假 HTTP 传输测试；emulator 不支持 REST data-plane） |
| `zeromq` | ZeroMQ | ZMTP 3.1（NULL） | 受限 | 已实现（P3；PUB/SUB、PUSH/PULL；协议/模拟测试，未跑真实 broker） |
| `nsq` | NSQ | NSQ TCP | 一等 | 已实现（P1；真实 NSQ 集成测试通过；HTTP 未实现） |
| `beanstalkd` | Beanstalkd | Beanstalkd 文本协议 | 一等 | 已实现（P1；真实 broker 集成测试通过） |

## 能力差异

| 驱动 | ack | requeue | delay | consumer group | 关键限制与选项 |
|---|---|---|---|---|---|
| rabbitmq | ✓ | ✓ | ✗ | ✗ | 默认交换机；无 publisher confirm |
| kafka 系 | ✓（提交 offset） | ✓（seek 回退） | ✗ | ✓ | 仅 PLAINTEXT/TLS、gzip；无 SASL；Event Hubs 需 SASL |
| activemq | ✓ | ✓（re-SEND + ACK） | ✓ | ✗ | `buffer_limit` 默认 10000；崩溃窗口内可能重复 |
| artemis | ✓ | ✗（STOMP NACK 等同 ACK 丢弃；应断开消费者重投） | ✓ | ✗ | `buffer_limit` 默认 10000；ARTEMIS-1983 |
| pulsar | ✓ | ✓（按条 negative ack） | ✓（约 1 秒分辨率） | ✓（Shared 订阅） | 实验；协议 v11；无 batching/压缩/schema |
| rocketmq | ✓ | ✓（重试队列） | ✓（延迟等级） | ✓ | 实验；`heartbeat_interval` 默认 30 秒；从 broker 已提交位点恢复；4.x remoting |
| nats | ✗ | ✗ | ✗ | ✓（配置 `queue_group` 时） | Core 为至多一次；`requiresActiveSubscription`；`buffer_limit` 默认 10000；空闲重连；JetStream 未实现 |
| redis | ✓（XACK） | 部分（保留 PEL，需 XCLAIM） | ✗ | ✓ | 首次消费从 0 开始，包含积压 |
| sqs | ✓（删除） | ✓（visibility=0） | ✓ | ✗ | 长轮询；FIFO 不支持逐条 delay |
| sns | — | — | ✗ | — | 仅 publish |
| kinesis | 无 ack（checkpoint） | ✗ | ✗ | 分片消费 | 仅内存 checkpoint + 可选回调 |
| gcp-pubsub | ✓ | ✓ | ✗ | ✓ | `ack_deadline_seconds` 默认 60（0–600，0 关闭续租）；单条 REST pull；attempts 恒为 1 |
| azure-service-bus | ✓（complete） | ✓（abandon） | ✓（scheduled） | ✗ | peek-lock；SAS 资源 URI 小写签名；实体需预建 |
| zeromq | ✗ | ✗ | ✗ | ✗ | 无持久化/确认；`buffer_limit` 默认 10000；PUB/SUB、PUSH/PULL |
| nsq | ✓（FIN） | ✓（REQ） | ✗（REQ 延迟仅用于 reject） | ✗ | channel 必须先存在；发布无 id |
| beanstalkd | ✓（delete） | ✓（release/bury） | ✓ | ✗ | 无 headers/key/批量；TTR 到期重投 |

## 关键连接选项

| 选项 | 适用驱动 | 默认 | 说明 |
|---|---|---|---|
| `buffer_limit` | activemq、artemis、nats、zeromq | 10000 | 按 destination 计数的内存缓冲上限；超限时抛 `ConnectionException`（STOMP）或 `ProtocolException`（NATS / ZeroMQ） |
| `heartbeat_interval` | rocketmq | 30.0 | 重复发送 `HEART_BEAT` 的间隔（秒），最小 1 |
| `ack_deadline_seconds` | gcp-pubsub | 60 | 拉取后延长消息租约的秒数，0 关闭额外请求，最大 600 |
| `subscription` | pulsar | messaging | Shared 订阅名；多个消费者可共享同一订阅，`reject(true)` 只重投被拒绝的消息 |

## 配置示例

各驱动的完整配置键、示例与限制见同目录 `drivers/` 下的驱动文档。通用规则：

- `driver` 必填，其余键由驱动读取；
- 连接级配置沿用数组风格，例如 `host` / `port` / `username` / `password` / `brokers` / `group_id` / `options`；
- 一个 `Manager` 可同时配置多个连接，`connection('name')` 按名取用；
- 云服务驱动的 `endpoint` 可指向本地模拟服务：AWS 使用 Moto，GCP 使用 Pub/Sub emulator。

## 集成验证

| CI 矩阵 | profile | 覆盖范围 |
|---|---|---|
| fast（push / PR） | `redis`、`beanstalkd`、`nsq`、`nats` | 真实 broker 容器 |
| heavy（schedule / workflow_dispatch） | `activemq`、`artemis`、`aws`、`gcp`、`rabbitmq`、`kafka`、`pulsar` | ActiveMQ、Artemis、AWS SQS/SNS/Kinesis（Moto）、GCP Pub/Sub（emulator）、RabbitMQ、Kafka（Redpanda）、Pulsar |

RocketMQ 与 ZeroMQ 目前只有单元/协议（假 broker）测试；Azure Service Bus 使用内存假 HTTP 传输，因为官方 emulator 不支持 REST data-plane。
