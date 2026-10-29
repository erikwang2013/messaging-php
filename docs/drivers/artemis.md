# ActiveMQ Artemis 驱动

驱动名：`artemis` · 命名空间：`Erikwang2013\Messaging\Driver\Stomp` · 协议：STOMP 1.2 over TCP。

与 ActiveMQ Classic 共用 `StompDriver`，差异在 `ArtemisPreset`：NACK 语义、延迟头与队列创建时机。

## 配置

| 键 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `host` | string | 必填 | broker 地址 |
| `port` | int | `61613` | STOMP acceptor 端口；docker-compose 将容器 61613 映射到宿主机 **61614** |
| `login` | string\|null | `admin` | CONNECT 登录名；`apache/artemis` 镜像默认是 `artemis` |
| `passcode` | string\|null | `admin` | CONNECT 密码；`apache/artemis` 镜像默认是 `artemis` |
| `prefix` | string | `/queue/` | 目标名前缀；目标名已以 `/queue/` 或 `/topic/` 开头时原样使用 |
| `timeout` | float | `5.0` | TCP 连接与 CONNECTED 握手超时（秒）；消费超时由 `ReceiveOptions::timeoutMs` 控制 |

```php
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Stomp\ArtemisFactory;
use Erikwang2013\Messaging\Manager;

$manager = new Manager([
    'default' => 'mq',
    'connections' => [
        'mq' => ['driver' => 'artemis', 'host' => '127.0.0.1', 'port' => 61614,
                 'login' => 'artemis', 'passcode' => 'artemis'],
    ],
]);
$manager->extend('artemis', new ArtemisFactory());

$connection = $manager->connection();
// Artemis 在消费者首次 attach 时创建队列；先订阅，避免发布被丢弃。
$connection->receive('orders', new ReceiveOptions(1));
$connection->publish('orders', new Message('payload'));

$message = $connection->receive('orders', new ReceiveOptions(5000));
$message?->ack();
```

## 能力

| 能力 | 支持 | 说明 |
|---|---|---|
| ack | ✓ | `ACK`（订阅使用 `ack:client-individual`） |
| requeue | ✗ | STOMP `NACK` 被当作 `ACK` 丢弃（ARTEMIS-1983）；`reject(true)` 抛 `UnsupportedFeatureException` |
| delay | ✓ | `AMQ_SCHEDULED_DELAY`（相对毫秒），由 Artemis STOMP 协议映射到调度投递 |
| consumer groups | ✗ | 同一地址的多个消费者竞争；协议无 group 概念 |
| batch publish | ✗ | 抛 `UnsupportedFeatureException` |
| ordering | ✗ | 不保证 FIFO |
| headers | ✓ | 标量 header；int/float/bool 经 `x-messaging-types` 保留类型 |
| requiresActiveSubscription | ✓ | 队列在消费者 attach 时创建；此前 `SEND` 到不存在的队列会被丢弃 |

### NACK / 重投限制

Artemis 的 STOMP `NACK` 与 `ACK` 等效，消息直接丢弃（ARTEMIS-1983）。驱动因此声明 `requeue = false`：

- `reject(false)` 发送 `ACK`，丢弃消息。
- `reject(true)` 在发送任何帧之前抛 `UnsupportedFeatureException`，提示通过关闭消费者连接触发重投。
- 连接断开时，未确认消息会由 broker 重新投递；驱动重连后清空本地缓冲并重新订阅，旧投递的 ack 闭包会抛 `ConnectionException`。

### 队列创建时机

Artemis 按地址/队列模型工作：`SEND` 会创建地址，但队列在第一个消费者 attach 时才创建；没有队列时消息被丢弃。因此 `requiresActiveSubscription = true`，共享契约测试会先订阅再发布。生产环境建议预创建队列，或先调用一次 `receive()` 建立订阅。

## 协议范围

- STOMP 1.2 帧：`COMMAND\nheader:value\n\n<body>\0`；header 按规范转义（`\\`、`\n`、`\r`、`\c`、`:`），CONNECT/STOMP/CONNECTED 帧不转义。接受 CRLF。
- 非空 body 自动写 `content-length`，二进制 body（含 NUL）安全往返。
- 握手：`STOMP` + `accept-version:1.2`、`host`、`login`/`passcode`、`heart-beat:0,0`（关闭心跳）。CONNECT ERROR 抛 `AuthenticationException`；版本协商失败抛 `ProtocolException`。
- 发布：`SEND` + `destination` + 自定义 header；`Message::key` 写入 `correlation-id`。`PublishResult::id` 是客户端生成的 32 位十六进制 id，同时写入 `x-messaging-id`。
- 延迟：`Message::delayMs > 0` 时写 `AMQ_SCHEDULED_DELAY`（相对毫秒）。
- 消费：每个目标保持一个持久 `SUBSCRIBE`（`id: sub-N`、`ack:client-individual`）；一次 `receive()` 只返回该目标的消息，其它目标的 MESSAGE 帧缓冲复用。
- 超时：`timeoutMs = 0` 永久阻塞；`> 0` 到期返回 `null`。读到半个帧后超时视为连接失步，抛 `ConnectionException` 并丢弃连接。
- `ack()` 发送 `ACK id:<MESSAGE 的 ack/message-id>`；`reject(false)` 也发 `ACK`。ACK 闭包绑定连接代次，重连后旧消息再确认会抛 `ConnectionException`。
- ERROR 帧在 `receive()` 时抛 `ProtocolException` 并断开；下次调用重新连接并重新订阅已跟踪的目标。`close()` 尽力发送 `DISCONNECT`。
- header 值仅支持标量；数组/对象抛 `UnsupportedFeatureException`。`destination`、`content-length`、`x-messaging-id`、`x-messaging-types`、`x-messaging-attempts` 为保留名。
- 未实现：事务、receipt、TLS、consumer group、批量发布。

## Manager 接入

`Manager` 已内置 `artemis` 驱动工厂。若使用自定义工厂实例，可覆盖注册：

```php
$manager->extend('artemis', new ArtemisFactory());
```
