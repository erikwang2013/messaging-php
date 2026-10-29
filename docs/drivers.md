# 驱动支持矩阵

20 个产品名由 13 套协议栈承载。状态随实施阶段更新。

| 驱动名 | 覆盖系统 | 协议 | 支持级别 | 状态 |
|---|---|---|---|---|
| `rabbitmq` | RabbitMQ | AMQP 0-9-1 | 一等 | 规划中 |
| `kafka` `redpanda` `automq` `confluent` `eventhubs` | Kafka、Redpanda、AutoMQ、Confluent Platform、Azure Event Hubs（Kafka 端点） | Kafka wire protocol | 一等（Event Hubs 复用 Kafka 端点） | 规划中 |
| `activemq` `artemis` | ActiveMQ Classic、ActiveMQ Artemis | STOMP 1.2 | 一等 | 规划中 |
| `pulsar` | Apache Pulsar | Pulsar 二进制协议 | 实验 | 规划中 |
| `rocketmq` | Apache RocketMQ 4.x/5.x | RocketMQ remoting | 实验 | 规划中 |
| `nats` | NATS Core + JetStream | NATS 文本协议 | 一等（Core）/ 实验（JetStream） | 规划中 |
| `redis` | Redis Streams | RESP | 一等 | 规划中 |
| `sqs` | AWS SQS | AWS JSON API + SigV4 | 一等 | 规划中 |
| `sns` | AWS SNS | AWS Query API + SigV4 | 仅发布 | 规划中 |
| `kinesis` | AWS Kinesis | AWS JSON API + SigV4 | 一等（简化 checkpoint） | 规划中 |
| `gcp-pubsub` | Google Cloud Pub/Sub | REST + 服务账号 JWT | 一等 | 规划中 |
| `azure-service-bus` | Azure Service Bus | REST + SAS | 一等 | 规划中 |
| `zeromq` | ZeroMQ | ZMTP 3.1（NULL） | 受限 | 规划中 |
| `nsq` | NSQ | NSQ TCP + HTTP | 一等 | 规划中 |
| `beanstalkd` | Beanstalkd | Beanstalkd 文本协议 | 一等 | 规划中 |

## 能力差异

| 驱动 | ack | requeue | delay | consumer group | 备注 |
|---|---|---|---|---|---|
| rabbitmq | ✓ | ✓ | ✗ | ✗ | publisher confirm、prefetch |
| kafka 系 | ✓（提交 offset） | ✓（seek 回退） | ✗ | ✓ | 仅 PLAINTEXT/TLS，gzip，无 SASL |
| activemq | ✓ | ✓（取决于 broker 配置） | ✓ | ✗ | STOMP ack/nack |
| artemis | ✓ | 受限（STOMP NACK 等同 ACK 丢弃；靠断开消费者重投） | ✓ | ✗ | ARTEMIS-1983 |
| pulsar | ✓ | ✓（negative ack） | ✓ | ✓ | v1 无 batching/压缩 |
| rocketmq | ✓ | ✓（重试队列） | ✓（延迟等级） | ✓ | 4.x remoting |
| nats | Core ✗ / JetStream ✓ | JetStream ✓ | ✗ | JetStream ✓ | Core 为至多一次 |
| redis | ✓（XACK） | 部分（保留 PEL，需 XCLAIM） | ✗ | ✓ | 待处理恢复需用户策略 |
| sqs | ✓（删除） | ✓（visibility=0） | ✓ | ✗ | 长轮询 |
| sns | — | — | ✗ | — | 仅 publish |
| kinesis | 无 ack（checkpoint） | ✗ | ✗ | 分片消费 | v1 仅内存 checkpoint + 可选回调 |
| gcp-pubsub | ✓ | ✓ | ✗ | ✓ | REST pull/ack |
| azure-service-bus | ✓（complete） | ✓（abandon） | ✓（scheduled） | ✗ | peek-lock |
| zeromq | ✗ | ✗ | ✗ | ✗ | 无持久化/确认 |
| nsq | ✓（FIN） | ✓（REQ） | ✓ | ✗ | |
| beanstalkd | ✓（delete） | ✓（release/bury） | ✓ | ✗ | TTR/优先级 |

## 配置示例

各驱动的配置键在对应阶段实现时补充到本节。通用规则：

- `driver` 必填，其余键由驱动读取；
- 连接级配置沿用数组风格，例如 `host` / `port` / `username` / `password` / `brokers` / `group_id` / `options`；
- 一个 `Manager` 可同时配置多个连接，`connection('name')` 按名取用。
