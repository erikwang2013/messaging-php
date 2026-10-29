# ActiveMQ Classic 驱动

驱动名：`activemq` · 命名空间：`Erikwang2013\Messaging\Driver\Stomp` · 协议：STOMP 1.2 over TCP。

与 Artemis 共用 `StompDriver`，差异只在 `ActiveMqPreset` 的端口、requeue 与延迟头。

## 配置

| 键 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `host` | string | 必填 | broker 地址 |
| `port` | int | `61613` | STOMP 连接器端口 |
| `login` | string\|null | `admin` | CONNECT 登录名；`null` 时不发送 |
| `passcode` | string\|null | `admin` | CONNECT 密码；`null` 时不发送 |
| `prefix` | string | `/queue/` | 目标名前缀；目标名已以 `/queue/` 或 `/topic/` 开头时原样使用 |
| `timeout` | float | `5.0` | TCP 连接与 CONNECTED 握手超时（秒）；消费超时由 `ReceiveOptions::timeoutMs` 控制 |

```php
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Stomp\ActiveMqFactory;
use Erikwang2013\Messaging\Manager;

$manager = new Manager([
    'default' => 'mq',
    'connections' => [
        'mq' => ['driver' => 'activemq', 'host' => '127.0.0.1', 'port' => 61613,
                 'login' => 'admin', 'passcode' => 'admin'],
    ],
]);
$manager->extend('activemq', new ActiveMqFactory());

$connection = $manager->connection();
$connection->publish('orders', new Message('payload', ['trace' => 'abc'], 'order-1'));

$message = $connection->receive('orders', new ReceiveOptions(5000));
$message?->ack();
```

## 能力

| 能力 | 支持 | 说明 |
|---|---|---|
| ack | ✓ | `ACK`（订阅使用 `ack:client-individual`） |
| requeue | ✓ | `reject(true)` 重新发布原消息后确认原投递；`reject(false)` 等同 `ACK` 丢弃 |
| delay | ✓ | `AMQ_SCHEDULED_DELAY`（相对毫秒）；broker 必须开启 `schedulerSupport="true"` |
| consumer groups | ✗ | 同一队列的多个消费者自然竞争；协议无 group 概念 |
| batch publish | ✗ | 抛 `UnsupportedFeatureException` |
| ordering | ✗ | 不保证 FIFO |
| headers | ✓ | 标量 header；int/float/bool 经 `x-messaging-types` 保留类型 |
| requiresActiveSubscription | ✗ | broker 在首次 `SEND` 时自动建队列 |

### requeue 为什么不是 NACK

ActiveMQ Classic 把 STOMP `NACK` 当作 poison ACK：STOMP `SEND` 默认发布非持久消息，默认 DLQ 策略会直接丢弃它们；持久消息则进入 `ActiveMQ.DLQ`。两种情况都不会回到原队列，且非持久消息是静默丢失。

因此 `reject(true)` 的实现是：按原 body、目标、自定义 header、`correlation-id`、`persistent`、`priority` 和 `x-messaging-id` 重新 `SEND`，成功后再 `ACK` 原投递。重投后的 `ReceivedMessage::attempts()` 递增（通过内部头 `x-messaging-attempts` 传递）。两步之间断线会重复投递（at-least-once），不会丢消息。需要 broker 侧 DLQ 语义时，请在发布时显式设置 `'persistent' => true`。

## 协议范围

- STOMP 1.2 帧：`COMMAND\nheader:value\n\n<body>\0`；header 按规范转义（`\\`、`\n`、`\r`、`\c`、`:`），CONNECT/STOMP/CONNECTED 帧不转义。接受 CRLF。
- 非空 body 自动写 `content-length`，二进制 body（含 NUL）安全往返。
- 握手：`STOMP` + `accept-version:1.2`、`host`、`login`/`passcode`、`heart-beat:0,0`（关闭心跳）。CONNECT ERROR 抛 `AuthenticationException`；版本协商失败抛 `ProtocolException`。
- 发布：`SEND` + `destination` + 自定义 header；`Message::key` 写入 `correlation-id`。`PublishResult::id` 是客户端生成的 32 位十六进制 id，同时写入 `x-messaging-id`；broker 不提供同步发布回执。
- 消费：每个目标保持一个持久 `SUBSCRIBE`（`id: sub-N`、`ack:client-individual`）；一次 `receive()` 只返回该目标的消息，其它目标的 MESSAGE 帧缓冲复用。
- 超时：`timeoutMs = 0` 永久阻塞；`> 0` 到期返回 `null`。读到半个帧后超时视为连接失步，抛 `ConnectionException` 并丢弃连接。
- `ack()` 发送 `ACK id:<MESSAGE 的 ack/message-id>`；`reject(false)` 也发 `ACK`。ACK 闭包绑定连接代次，重连后旧消息再确认会抛 `ConnectionException`（broker 会重投未确认消息）。
- ERROR 帧在 `receive()` 时抛 `ProtocolException` 并断开；下次调用重新连接并重新订阅已跟踪的目标。`close()` 尽力发送 `DISCONNECT`。
- header 值仅支持标量；数组/对象抛 `UnsupportedFeatureException`。`destination`、`content-length`、`x-messaging-id`、`x-messaging-types`、`x-messaging-attempts` 为保留名。
- 未实现：事务、receipt、TLS、consumer group、批量发布。

## Manager 接入

`Manager` 已内置 `activemq` 驱动工厂。若使用自定义工厂实例，可覆盖注册：

```php
$manager->extend('activemq', new ActiveMqFactory());
```
