# NSQ 驱动

纯 PHP 实现的 NSQ v2 TCP 协议客户端（`Erikwang2013\Messaging\Driver\Nsq`），不依赖 nsq 官方 SDK。

## 配置

```php
use Erikwang2013\Messaging\Manager;
use Erikwang2013\Messaging\Driver\Nsq\NsqFactory;

$manager = new Manager([
    'default' => 'nsq',
    'connections' => [
        'nsq' => [
            'driver' => 'nsq',
            'host' => '127.0.0.1',   // 必填
            'port' => 4150,          // 默认 4150
            'channel' => 'default',  // 默认 default，同一 topic 的多个消费者可共用该 channel 竞争消费
            'timeout' => 5.0,        // 连接与握手读超时（秒），默认 5.0
            'requeue_delay_ms' => 0, // reject(true) 的 REQ 延迟，默认 0
        ],
    ],
]);
$manager->extend('nsq', new NsqFactory());

$conn = $manager->connection('nsq');
```

`destination` 即 NSQ topic。一个驱动实例复用一个发布连接，并为每个消费 topic 各保持一个消费连接（NSQ 单连接只允许一次 `SUB`）。

## 能力

| 能力 | 支持 | 说明 |
|---|---|---|
| ack | ✓ | `FIN <id>` |
| requeue | ✓ | `REQ <id> <delay>`；`reject(false)` 等同 `FIN`（丢弃） |
| delay（发布） | ✗ | 发布带 `delayMs` 抛 `UnsupportedFeatureException` |
| consumer group | ✗ | NSQ channel 本身即竞争消费语义，但驱动不暴露 group 管理 |
| batch publish | ✗ | 抛 `UnsupportedFeatureException` |
| ordering | ✗ | 不保证顺序 |
| headers | ✗ | 非空 headers 或 key 抛 `UnsupportedFeatureException`，不会静默丢弃 |

`ReceivedMessage::attempts()` 来自消息帧里的 attempts 字段；第一次投递为 1。

## 协议范围

- 连接：发送 4 字节 magic `  V2`
- 发布：`PUB <topic>\n` + 4 字节大端 body 长度 + body，等待 `OK`；`E_*` 错误帧转为 `ProtocolException`
- 消费：`SUB <topic> <channel>\n`，每次阻塞读取前发送 `RDY 1\n`
- 帧格式：4 字节大端 size（含 4 字节 frame type）+ 4 字节 type + payload
  - type 0 = response（`OK`、`_heartbeat_`）
  - type 1 = error（`E_*`）
  - type 2 = message（8 字节纳秒时间戳 + 2 字节 attempts + 16 字节 message id + body）
- 心跳：读到 `_heartbeat_` 立即回 `NOP\n`
- 确认：`FIN <id>\n`；重投：`REQ <id> <delayMs>\n`

## 限制

- **channel 必须先存在**：NSQ 只把消息投递给发布时已存在的 channel。驱动在首次 `receive()` 时惰性 `SUB` 创建 channel，因此「先发布、后首次订阅」的消息会丢失。消费者进程应先调用一次 `receive()`（可用短超时）完成订阅，再开始发布/消费；集成测试即按此顺序验证。
- **发布无 message id**：NSQ 在投递时才生成 16 字节 id，`PublishResult::id` 恒为 `null`。
- **拉取模型与心跳**：驱动只在 `receive()`/握手/等待发布应答期间读取 socket 并回应心跳。若两次 `receive()` 之间（或处理消息期间）超过 nsqd 心跳超时（默认约 60 秒），消费连接会被 broker 关闭，之后 `ack()`/`reject()` 或下一次 `receive()` 抛 `ConnectionException`。长任务请缩短轮询间隔，或调大 nsqd `--heartbeat-interval`。
- **发布连接空闲**：发布连接空闲超过约 60 秒同样会被 nsqd 关闭。此时本次 `publish()` 抛 `ConnectionException`，驱动会丢弃旧连接，下一次调用自动重连；不会自动重试，以免产生重复消息。
- **超时不破坏连接**：读取超时返回 `null`，已读到的半帧保留在缓冲区，下一次 `receive()` 继续解析；超时不会关闭连接。
- 不支持 TLS、AUTH、ephemeral channel、`DPUB`、`TOUCH`、消息体压缩。

## 测试

```bash
vendor/bin/phpunit --testsuite unit --filter Nsq
docker compose --profile nsq up -d
vendor/bin/phpunit --testsuite integration --filter NsqTest
```

集成测试读取 `MESSAGING_NSQD_HOST`（默认 `127.0.0.1`）与 `MESSAGING_NSQD_PORT`（默认 `4150`），连接失败时自动 skip。
