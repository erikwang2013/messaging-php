# NATS 驱动

驱动名：`nats` · 命名空间：`Erikwang2013\Messaging\Driver\Nats` · 实现：`NatsDriver` / `NatsFactory`

当前实现范围：**NATS Core 文本协议**。JetStream（持久化、ack、重投、延迟、消费者组持久订阅）不在本阶段范围内，见文末。

## 配置

| 键 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `host` | 是 | — | 服务器地址 |
| `port` | 否 | `4222` | 服务器端口（整数或数字字符串） |
| `user` | 否 | null | CONNECT 用户名；设置 `pass` 时必须同时设置 `user` |
| `pass` | 否 | null | CONNECT 密码 |
| `queue_group` | 否 | null | 订阅使用的 queue group；非空时 `consumerGroups` 能力为 true |
| `timeout` | 否 | `5.0` | 连接、握手与帧读取超时（秒） |
| `headers` | 否 | `true` | 是否启用 headers；服务器不支持时自动降为 false |
| `buffer_limit` | 否 | `10000` | 每个 destination 的待取消息缓冲上限（正整数）；超限抛 `ProtocolException` |

TLS 暂未开放配置（`tls_required` 固定为 false），TLS 端点留待后续版本。

```php
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Manager;

$mq = new Manager([
    'default' => 'nats',
    'connections' => [
        'nats' => [
            'driver' => 'nats',
            'host' => '127.0.0.1',
            'port' => 4222,
            'queue_group' => 'workers', // 可选
        ],
    ],
]);
$connection = $mq->connection();

// Core NATS 只向已存在的订阅投递：先建立订阅，再发布。
$connection->receive('orders.created', new ReceiveOptions(1));

$connection->publish('orders.created', new Message('hello'));
$message = $connection->receive('orders.created', new ReceiveOptions(1000));
```

## 能力

| 能力 | 值 | 说明 |
|---|---|---|
| `ack` | false | Core NATS 至多一次，投递即消费；`ack()` 抛 `UnsupportedFeatureException` |
| `requeue` | false | 不支持；`reject()` 抛 `UnsupportedFeatureException` |
| `delay` | false | 不支持延迟投递；`Message::$delayMs` 非空时抛 `UnsupportedFeatureException` |
| `consumerGroups` | 配置 `queue_group` 时 true | 通过 `SUB <subject> <queue> <sid>` 实现 |
| `batchPublish` | false | `publishBatch()` 抛 `UnsupportedFeatureException` |
| `ordering` | true | 单连接内按发布顺序投递 |
| `headers` | 默认 true | 需服务器 INFO 声明 `headers:true`，否则自动降为 false；header 值为字符串 |
| `requiresActiveSubscription` | true | 只向发布前已建立的订阅投递 |

## 协议范围

- 握手：读取 `INFO {json}`，发送 `CONNECT {...}`（含 `headers:true`）与 `PING`，等待 `PONG`。
- 发布：无 headers 时 `PUB <subject> [reply] <bytes>\r\n<body>\r\n`；有 headers 时
  `HPUB <subject> [reply] <header-bytes> <total-bytes>\r\nNATS/1.0\r\n...\r\n\r\n<body>\r\n`。
  每次发布后以 `PING`/`PONG` 冲刷，确保已写入服务器。发布/冲刷遇到 `ConnectionException`
  或 `TimeoutException`（如连接被服务器的 ping 窗口关闭）时，驱动关闭旧 socket、重新握手，
  并把同一帧重发一次；Core NATS 至多一次，重发可能产生重复投递。
- 订阅：收到 `MSG`/`HMSG` 后缓冲，由 `receive()` 取出；第一次 `receive()` 某个 destination
  时才发送 `SUB <subject> [queue] <sid>`，因此**发布前必须先订阅**。`receive()` 的
  `timeoutMs` 为 0 时一直阻塞，超时返回 `null`。
- 控制帧：响应服务器心跳 `PING`（回 `PONG`），忽略 `+OK`/`INFO`，`-ERR` 映射为
  `ProtocolException`，认证类错误映射为 `AuthenticationException`。
- 连接失败抛 `ConnectionException`，读取超时抛 `TimeoutException`（`receive()` 自身的
  超时除外），畸形帧抛 `ProtocolException`。
- body 二进制安全，支持零长度消息。

## 语义与限制

- **至多一次**：订阅采用 Core NATS 的即时投递，没有持久化；消费者离线期间的消息会丢失。
- `ReceivedMessage::ack()` 与 `reject()` 都抛 `UnsupportedFeatureException`（能力为
  `ack:false` / `requeue:false`），驱动不会静默忽略确认请求。
- Core NATS 没有消息 id / key 概念：驱动在发布时生成 id，并把 id 与 key 编码进
  `Messaging-Id` / `Messaging-Key` 头，接收时还原并从 `headers()` 中移除。因此这两个
  字段要求服务器与连接双方都支持 headers；服务器不支持 headers 时 `publish()` 返回的
  id 为 `null`。
- header 值在 NATS 线上只能是字符串：发布时数字、布尔值、浮点数会转成字符串，接收时
  **一律按字符串返回**（`['attempt' => 2]` 会变成 `['attempt' => '2']`），驱动不会猜测类型。
- **必须先订阅再发布**（`requiresActiveSubscription: true`）：`publish()` 不会隐式创建
  订阅。同一连接想取回自己发布的消息，也要先调用一次 `receive()`（即使是 1ms 超时）建立
  订阅；跨连接消费同理，订阅方必须先启动。
- **缓冲上限**：每个 destination 最多缓存 `buffer_limit`（默认 10000）条尚未被
  `receive()` 取走的消息；接收 A 时到达的 B 消息、以及发布冲刷期间到达的消息都会占用
  该额度。超过上限抛 `ProtocolException`，避免慢消费者无限占用内存。该异常不会被发布重试
  逻辑当作断线处理。
- **空闲重连**：服务器关闭空闲连接（ping 窗口）后，下一次 `publish()` 会自动重连并重发
  一次；重连后旧订阅失效（订阅是连接级的），需要重新 `receive()` 建立。
- 每次 `publish()` 都做一次 PING/PONG 往返，吞吐低于批量客户端；这是可靠交付边界的取舍。
- 未配置 `queue_group` 时每个连接都是独立订阅者，消息会广播给所有订阅者。

## JetStream（后续阶段）

JetStream 需要额外的 API（`$JS.API.>` 请求、stream/consumer 创建、pull 订阅、
ack/NAK/term、延迟与重投）。本驱动不包含这些能力，也不会静默降级：需要持久化或确认
语义时请使用 JetStream 客户端或等待后续版本。

## 测试

```bash
vendor/bin/phpunit --testsuite unit --filter Nats
docker compose --profile nats up -d
vendor/bin/phpunit --testsuite integration --filter Nats
```

集成测试读取 `MESSAGING_NATS_HOST`（默认 `127.0.0.1`）与 `MESSAGING_NATS_PORT`
（默认 `4222`），无法连接时自动 skip。
