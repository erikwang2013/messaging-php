# messaging-php 实施规划

## Context

在 `/home/wwwroot/erikwang2013` 新建一个 Composer 库，用一个统一 API 支持 20 种消息队列 / 事件流系统的 PHP 端接入：

> RabbitMQ、Kafka、RocketMQ、Pulsar、ActiveMQ、ActiveMQ Artemis、NATS、Redpanda、AutoMQ、Redis Streams、AWS SQS、AWS SNS、AWS Kinesis、Google Cloud Pub/Sub、Azure Service Bus、Azure Event Hubs、ZeroMQ、NSQ、Beanstalkd、Confluent Platform

工作区已有 30+ 个同类 PHP 客户端项目（clickhouse-php、consul-php、flink-php 及 bigdata 系列），统一惯例：每个项目独立子目录 + 独立 git 仓库、包名 `erikwang2013/<name>-php`、命名空间 `Erikwang2013\<Name>\`、PHP ≥ 8.1、`declare(strict_types=1)`、PSR-4/PSR-3、Guzzle、PHPUnit 10.5、MIT、中文 README、集成测试 `@group integration` + docker-compose。

**已确认决策**
- 目录：`/home/wwwroot/erikwang2013/messaging-php`，包名 `erikwang2013/messaging-php`，命名空间 `Erikwang2013\Messaging`
- 单包，所有驱动内置；不依赖任何 broker SDK / PHP 扩展
- 尽量纯 PHP 自研协议实现（TCP 用 `stream_socket_client`，HTTP 层沿用工作区惯例 Guzzle，TLS 用 ext-openssl，gzip 用 ext-zlib）
- 这里的“composer 插件”按 Composer 库（`type: library`）理解，不是 `composer-plugin` 生命周期插件

## 目标 / 非目标

**目标**
- 一套 `Manager → Connection → publish/receive/ack` API 覆盖全部 20 个系统
- 纯 PHP 实现，`composer require` 后无需安装 broker SDK 或额外扩展（除 PHP 标准 openssl/zlib）
- 每个驱动有统一的契约测试，集成测试对真实 broker 容器运行
- 能力（ack / requeue / delay / consumer group / batch）显式声明，不支持时直接抛异常而不是静默忽略

**非目标（v1 不做）**
- Schema Registry、事务、exactly-once、消息轨迹、管理台 / topic 拓扑管理
- 异步 / Fiber / Swoole 专用 API（阻塞式同步 API，适配常驻 CLI worker）
- Laravel / Hyperf / webman / ThinkPHP / Yii2 / Yii3 适配器（后续版本）
- 与 C 扩展（rdkafka）持平的高吞吐；Kafka SASL、非 gzip 压缩、Pulsar/RocketMQ 高级特性
- 集群运维能力（分区再均衡以外的管理接口）

## 20 个系统 → 13 套协议栈

| # | 对外驱动名 | 覆盖系统 | 协议 / 传输 | 实现方式 | 支持级别 |
|---|---|---|---|---|---|
| 1 | `rabbitmq` | RabbitMQ | AMQP 0-9-1 over TCP/TLS | 自研帧编解码 | 一等 |
| 2 | `kafka` `redpanda` `automq` `confluent` `eventhubs` | Kafka、Redpanda、AutoMQ、Confluent、Azure Event Hubs（Kafka 端点） | Kafka wire protocol | 自研最小子集 | Kafka 一等、别名同栈；Event Hubs 走 Kafka 端点 |
| 3 | `activemq` `artemis` | ActiveMQ Classic、ActiveMQ Artemis | STOMP 1.2 over TCP/TLS | 自研 | 一等 |
| 4 | `pulsar` | Apache Pulsar | Pulsar 二进制协议 + protobuf wire | 自研最小子集 | 实验 |
| 5 | `rocketmq` | Apache RocketMQ 4.x/5.x | RocketMQ remoting（5.x 兼容） | 自研 | 实验 |
| 6 | `nats` | NATS（Core + JetStream） | NATS 文本协议 | 自研 | 一等（Core）；JetStream 实验 |
| 7 | `redis` | Redis Streams | RESP | 自研 | 一等 |
| 8 | `sqs` | AWS SQS | AWS JSON API + SigV4 | 自研签名 + Guzzle | 一等 |
| 9 | `sns` | AWS SNS | AWS Query/XML API + SigV4 | 自研签名 + Guzzle | 仅发布（SNS 无拉取消费） |
| 10 | `kinesis` | AWS Kinesis | AWS JSON API + SigV4 | 自研签名 + Guzzle | 一等（checkpoint 简化） |
| 11 | `gcp-pubsub` | Google Cloud Pub/Sub | REST + 服务账号 JWT（RS256） | 自研签名 + Guzzle | 一等 |
| 12 | `azure-service-bus` | Azure Service Bus | REST + SAS Token | 自研签名 + Guzzle | 一等 |
| 13 | `zeromq` | ZeroMQ | ZMTP 3.1（NULL mechanism） | 自研子集 | PUB/SUB、PUSH/PULL；无持久化/ack |
| 14 | `nsq` | NSQ | NSQ TCP + HTTP | 自研 | 一等 |
| 15 | `beanstalkd` | Beanstalkd | Beanstalkd 文本协议 | 自研 | 一等 |

> 13 套协议栈承载 20 个产品名：Kafka 兼容系 5 个共用一个驱动，ActiveMQ/Artemis 共用一个 STOMP 驱动。驱动类按对外名称注册，别名只改默认端口/配置预设。

### 能力和语义差异（必须在 docs/drivers.md 固化）

| 驱动 | ack | requeue | delay | consumer group | 备注 |
|---|---|---|---|---|---|
| rabbitmq | ✓ | ✓ | ✗（需插件，v1 不做） | ✗（竞争消费即可） | publisher confirm、prefetch |
| kafka 系 | ✓（提交 offset） | ✓（seek 回退） | ✗ | ✓ | 仅 PLAINTEXT/TLS，gzip，无 SASL |
| activemq | ✓ | ✓（NACK 重投/DLQ，取决于 broker 配置） | ✓（broker 定时头） | ✗ | 基于 STOMP ack/nack |
| artemis | ✓ | 受限（STOMP NACK 会被当 ACK 丢弃，见 ARTEMIS-1983；只能靠断开消费者触发重投） | ✓ | ✗ | 能力表标注限制 |
| pulsar | ✓ | ✓（negative ack） | ✓ | ✓ | 无 batching/压缩（v1） |
| rocketmq | ✓ | ✓（重试队列） | ✓（延迟等级） | ✓ | 4.x remoting |
| nats | Core ✗ / JetStream ✓ | JetStream ✓ | ✗ | JetStream ✓ | Core 为至多一次 |
| redis | ✓（XACK） | 部分（保留 PEL，需 XCLAIM） | ✗ | ✓（consumer group） | 待处理消息恢复需用户侧策略 |
| sqs | ✓（删除） | ✓（visibility=0） | ✓ | ✗ | 长轮询 |
| sns | — | — | ✗ | — | 仅 publish，receive 抛 UnsupportedFeature |
| kinesis | 无 ack（checkpoint） | ✗ | ✗ | 分片消费 | v1 只做内存 checkpoint + 可选回调存储 |
| gcp-pubsub | ✓ | ✓ | ✗ | ✓ | REST pull/ack |
| azure-service-bus | ✓（complete） | ✓（abandon） | ✓（scheduled） | ✗ | peek-lock |
| zeromq | ✗ | ✗ | ✗ | ✗ | 无持久化、无确认 |
| nsq | ✓（FIN） | ✓（REQ） | ✓ | ✗ | |
| beanstalkd | ✓（delete） | ✓（release/bury） | ✓ | ✗ | TTR/优先级 |

## 架构

### 公开 API（尽量小）

```php
$mq = new Manager([                          // 或 Manager::fromArray()
    'default' => 'rabbit',
    'connections' => [
        'rabbit' => ['driver' => 'rabbitmq', 'host' => '127.0.0.1', 'port' => 5672,
                     'username' => 'guest', 'password' => 'guest', 'vhost' => '/'],
        'kafka'  => ['driver' => 'kafka', 'brokers' => ['127.0.0.1:9092'],
                     'group_id' => 'app', 'options' => ['auto_offset_reset' => 'earliest']],
    ],
], $logger);

$conn = $mq->connection('rabbit');
$conn->publish('orders', Message::json(['id' => 1], ['trace' => 'abc']));

while ($msg = $conn->receive('orders', new ReceiveOptions(timeoutMs: 5000))) {
    try {
        handle($msg->body(), $msg->headers());
        $msg->ack();
    } catch (Throwable $e) {
        $msg->reject(requeue: true);
    }
}
```

契约：

```php
final class Manager {
    public function __construct(array|Config $config, ?LoggerInterface $logger = null);
    public static function fromEnv(): self;
    public function connection(?string $name = null): Connection;
    public function extend(string $driver, DriverFactoryInterface $factory): void;
}

final class Connection {
    public function publish(string $destination, Message|string $message, array $options = []): PublishResult;
    public function publishBatch(string $destination, array $messages, array $options = []): array; // 不支持时抛异常
    public function receive(string $destination, ?ReceiveOptions $options = null): ?ReceivedMessage;
    public function consume(string $destination, callable $handler, ?ConsumeOptions $options = null): void;
    public function capabilities(): Capabilities;
    public function close(): void;
}

final class Message {
    public function __construct(
        public readonly string $body,
        public readonly array $headers = [],
        public readonly ?string $key = null,
        public readonly ?int $delayMs = null,
    ) {}
    public static function json(array $data, array $headers = [], ?string $key = null, ?int $delayMs = null): self;
}

final class ReceivedMessage {
    public function body(): string;
    public function headers(): array;
    public function id(): ?string;
    public function key(): ?string;
    public function attempts(): int;
    public function ack(): void;
    public function reject(bool $requeue = false): void;
    public function raw(): mixed;              // 逃逸口：底层协议消息/客户端对象
}

final class Capabilities {
    // readonly bool: ack, requeue, delay, consumerGroups, batchPublish, ordering, headers
}
```

- 消费模型统一为 **拉取式** `receive()`；push 型协议（AMQP basic.consume、ZeroMQ SUB、NATS SUB）在驱动内部缓冲后由 `receive()` 取出。`consume()` 只是循环糖。
- 配置以数组为主（工作区惯例），`Config::fromArray()` 做校验；可选 `dsn` 字段（`amqp://...`、`kafka://...`、`redis://...`）在后续小版本加，v1 先支持数组。
- 驱动工厂在创建时检查 PHP 扩展（openssl/zlib）和必填配置，失败抛 `ConnectionException` / `UnsupportedFeatureException`，错误信息直接给出修复方法。

### 异常

```
MessagingException (RuntimeException)
├── ConnectionException
│   └── TimeoutException
├── ProtocolException          // 畸形帧 / 协议状态错误
├── AuthenticationException
├── RequestException           // HTTP 非 2xx，携带 status/body（工作区惯例）
└── UnsupportedFeatureException
```

### 目录结构

```
messaging-php/
├── composer.json
├── phpunit.xml
├── .gitignore
├── LICENSE
├── README.md                     # 中文，驱动矩阵 + 快速开始 + 支持级别
├── docker-compose.yml            # 按 profile 启停各 broker
├── src/
│   ├── Messaging.php             # 静态门面（可选）
│   ├── Manager.php
│   ├── Connection.php
│   ├── Config/{Config,ConnectionConfig}.php
│   ├── Contract/{DriverInterface,DriverFactoryInterface,Message,ReceivedMessage,
│   │             Capabilities,PublishResult,ReceiveOptions,ConsumeOptions}.php
│   ├── Exception/*.php
│   ├── Support/{Socket,Http,Json,Backoff,Checksum,Protobuf,Signer}.php
│   └── Driver/
│       ├── RabbitMq/{RabbitMqDriver,RabbitMqFactory,Amqp/...}
│       ├── Kafka/{KafkaDriver,KafkaFactory,Kafka/...}
│       ├── Stomp/{StompDriver,StompFactory,ActiveMqPreset,ArtemisPreset}
│       ├── Pulsar/{PulsarDriver,Pulsar/...}
│       ├── RocketMq/{RocketMqDriver,RocketMq/...}
│       ├── Nats/{NatsDriver,Nats/...}
│       ├── Redis/{RedisStreamsDriver,RedisStreamsFactory}
│       ├── Aws/{SqsDriver,SnsDriver,KinesisDriver,SigV4,HttpApi}
│       ├── Gcp/{PubSubDriver,ServiceAccountJwt}
│       ├── Azure/{ServiceBusDriver,SasToken}
│       ├── ZeroMq/{ZeroMqDriver,Zmtp/...}
│       ├── Nsq/{NsqDriver,Nsq/...}
│       └── Beanstalkd/{BeanstalkdDriver,Beanstalkd/...}
├── tests/
│   ├── Unit/                     # 编解码金字节、配置、签名向量、能力
│   ├── Protocol/                 # 用 stream_socket_server 起的假 broker
│   ├── Contract/                 # 所有驱动共用的契约测试基类
│   └── Integration/              # @group integration，连真实容器
├── docs/
│   ├── drivers.md                # 20 系统支持矩阵、限制、配置示例
│   └── protocols.md              # 各协议实现范围与版本假设
└── examples/                     # 每个驱动一个最小示例
```

## 分阶段实施

| 阶段 | 内容 | 验收标准 | 粗估 |
|---|---|---|---|
| **P0 骨架** | composer/phpunit/CI/LICENSE/README 骨架；`Manager/Connection/Config`、契约、异常、`Support/Socket,Http,Json,Backoff,Checksum`；契约测试基类 + 假 broker harness | `composer validate`、`composer test` 绿、PHPStan level 5、无 broker SDK 依赖 | 2–3 人日 |
| **P1 简单协议** | Beanstalkd、Redis Streams、NSQ、STOMP（ActiveMQ/Artemis）、NATS Core | 各驱动契约测试 + docker profile 集成测试：publish→receive→ack/reject、超时、能力声明一致 | 8–12 人日 |
| **P2 HTTP 云服务** | `Support/Http`（Guzzle 封装）、AWS SigV4、GCP 服务账号 JWT、Azure SAS；SQS/SNS/Kinesis、GCP Pub/Sub、Azure Service Bus | SigV4 官方测试向量通过；JWT 签名可被校验；LocalStack（SQS/SNS/Kinesis）、Pub/Sub emulator 集成测试；Azure emulator 可选 | 6–9 人日 |
| **P3 AMQP + ZeroMQ** | AMQP 0-9-1（帧、channel、exchange/queue、publish confirm、consume、heartbeat、TLS）；ZMTP 3.1 子集（NULL、PUB/SUB、PUSH/PULL） | RabbitMQ 容器契约测试（ack/redelivery/confirm/headers/TLS）；pyzmq peer 容器测 ZMTP；能力旗标正确 | 8–12 人日 |
| **P4 Kafka 家族** | Kafka 协议最小集：ApiVersions/Metadata/Produce/Fetch/ListOffsets/FindCoordinator/JoinGroup/SyncGroup/Heartbeat/LeaveGroup/OffsetCommit/OffsetFetch；record batch v2、CRC32C、gzip、TLS、headers/key；消费者组再均衡；别名预设 | 对 apache/kafka 与 Redpanda 容器跑契约测试；双消费者再均衡；offset 持久化；record batch 金字节测试；无 SASL（文档声明） | 12–18 人日 |
| **P5 Pulsar + RocketMQ** | Pulsar 二进制协议最小集（protobuf wire：Connect/Lookup/Producer/Send/Subscribe/Flow/Ack）；RocketMQ 4.x remoting（nameserver、producer、pull consumer、ack/重试） | Pulsar standalone、RocketMQ 5.x（namesrv+broker）容器契约测试；README 标注 experimental | 15–25 人日 |
| **P6 加固发布** | 全矩阵契约跑通、docs/drivers.md、示例、性能说明、v1.0.0 tag | 全部驱动能力表与实现一致；CI 全绿；README 快速开始可复制运行 | 3–5 人日 |

> 总量约 55–85 人日。P4/P5 是纯 PHP 自研的主要风险点；若工期优先，驱动接口允许后续在同一 `DriverInterface` 下替换为 rdkafka / 官方客户端实现，而不改公开 API。

## 测试策略

1. **单元测试（无外部依赖）**：协议帧编解码金字节（AMQP 帧、Kafka record batch + CRC32C、STOMP 帧、RESP、ZMTP）、DSN/配置校验、SigV4 官方测试向量、JWT RS256、能力表。
2. **协议测试**：`tests/Protocol/` 用 `stream_socket_server` 起脚本化假 broker，覆盖正常响应、畸形帧、超时、断连重连；不需要 Docker。
3. **契约测试**：`tests/Contract/DriverContractTestCase.php` 定义统一行为（发布/接收/ack/reject/requeue/超时/大报文/二进制 body/headers/key），每个驱动继承并只负责创建连接。
4. **集成测试**：`@group integration`，环境变量或 `docker-compose --profile <broker>` 提供真实服务；缺服务时自动 skip，CI 显式开启。
5. **CI**：PHP 8.1–8.5 跑单测 + PHPStan + CS；集成测试按 profile 拆 job（redis/beanstalkd/nsq/nats/activemq/rabbitmq/localstack/pubsub/kafka/redpanda/pulsar/rocketmq），Pulsar/RocketMQ 可放 nightly。

## composer.json 草案

```json
{
    "name": "erikwang2013/messaging-php",
    "description": "Pure-PHP messaging client for RabbitMQ, Kafka, RocketMQ, Pulsar, ActiveMQ, Artemis, NATS, Redpanda, AutoMQ, Redis Streams, AWS SQS/SNS/Kinesis, Google Pub/Sub, Azure Service Bus/Event Hubs, ZeroMQ, NSQ and Beanstalkd.",
    "type": "library",
    "license": "MIT",
    "require": {
        "php": ">=8.1",
        "guzzlehttp/guzzle": "^7.0",
        "psr/log": "^3.0"
    },
    "require-dev": {
        "phpunit/phpunit": "^10.5",
        "phpstan/phpstan": "^1.11",
        "friendsofphp/php-cs-fixer": "^3.0"
    },
    "autoload": { "psr-4": { "Erikwang2013\\Messaging\\": "src/" } },
    "autoload-dev": { "psr-4": { "Erikwang2013\\Messaging\\Tests\\": "tests/" } },
    "scripts": {
        "test": "phpunit --testsuite unit",
        "test-integration": "phpunit --testsuite integration"
    },
    "config": { "sort-packages": true },
    "minimum-stability": "stable"
}
```

## 主要风险

1. **纯 PHP Kafka/Pulsar/RocketMQ 的协议复杂度**——只实现稳定最小子集，明确版本假设；契约测试 + 金字节测试兜底；标注 experimental；驱动接口留替换空间。
2. **性能**：纯 PHP 吞吐远低于 rdkafka，尤其 Kafka/Pulsar。文档给出量级说明，建议多进程 worker 水平扩展；不做"高性能"承诺。
3. **协议漂移**：broker 新版本可能改 API；固定已验证的 API 版本，ApiVersions 协商失败时给出明确错误。
4. **ZeroMQ 语义缺失**：无持久化/ack，能力旗标必须为 false，文档醒目标注。
5. **Artemis 的 STOMP NACK 不可靠**（ARTEMIS-1983：NACK 等同 ACK 丢弃），`reject(requeue: true)` 只能通过关闭消费者连接触发重投；能力表和文档必须如实说明，契约测试单独覆盖。
6. **SNS 无法消费**：仅 publish；`receive()` 抛 `UnsupportedFeatureException`。
7. **Kinesis checkpoint**：v1 只做内存 checkpoint + 可选回调存储，不假装有 ack。
8. **Azure/GCP 集成测试重量**：emulator 可用性不一；签名与请求构造用单元测试覆盖，真实服务测试为可选 job。
9. **集成测试矩阵大**：按 profile 拆分，PR 只跑核心子集，nightly 跑全量。

## 验证方式

```bash
cd /home/wwwroot/erikwang2013/messaging-php
composer validate
composer test                        # 单元 + 协议测试
docker compose --profile redis up -d
vendor/bin/phpunit --group integration --filter RedisStreams
docker compose --profile rabbitmq up -d
vendor/bin/phpunit --group integration --filter RabbitMq
php examples/redis-streams.php       # 每个驱动的示例脚本
```

验收：20 个驱动名全部可实例化；支持的驱动在真实容器上完成 publish→receive→ack/reject 往返；docs/drivers.md 的能力表与实际行为逐项一致；`composer require` 不引入任何 broker SDK 或非标准扩展。
