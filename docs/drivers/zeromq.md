# ZeroMQ 驱动

驱动名：`zeromq` · 命名空间：`Erikwang2013\Messaging\Driver\ZeroMq` · 实现：`ZeroMqDriver` / `ZeroMqFactory`

当前实现范围：**ZMTP 3.1 + NULL 机制**，支持 PUB/SUB 与 PUSH/PULL 两种模式。纯 PHP 实现，不依赖 ext-zmq。ZeroMQ 是 brokerless、至多一次的：没有持久化、ack、重投、延迟和消费者组。

## 配置

| 键 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `host` | 是 | — | 对端地址 |
| `port` | 是 | — | 对端端口（整数或数字字符串） |
| `pattern` | 否 | `pubsub` | `pubsub`：publish 使用 PUB socket、receive 使用 SUB socket；`pushpull`：publish 使用 PUSH、receive 使用 PULL |
| `identity` | 否 | null | ZMTP READY 中携带的 `Identity` 元数据 |
| `timeout` | 否 | `5.0` | 连接、握手与单次帧读取超时（秒） |
| `tcp_nodelay` | 否 | `true` | 启用 TCP_NODELAY。订阅帧很小，关闭后可能被 Nagle 延迟 |
| `buffer_limit` | 否 | `10000` | 每个非目标 destination 在内存中最多缓存的消息数；超出时抛 `ProtocolException` |

```php
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Manager;

$mq = new Manager([
    'default' => 'zmq',
    'connections' => [
        'zmq' => [
            'driver' => 'zeromq',
            'host' => '127.0.0.1',
            'port' => 5555,
            'pattern' => 'pubsub',
        ],
    ],
]);
$connection = $mq->connection();

// 订阅端：先注册 topic 前缀（PUB/SUB 只向已存在的订阅投递）
$connection->receive('orders.created', new ReceiveOptions(10));
$message = $connection->receive('orders.created', new ReceiveOptions(1000));

// 发布端
$connection->publish('orders.created', new Message('hello'));
```

`publish()` 与 `receive()` 各自懒建立一条 TCP 连接：PUB/SUB 模式下分别是 PUB 与 SUB，PUSH/PULL 模式下分别是 PUSH 与 PULL。真实 libzmq 端点通常只绑定一种 socket 类型，所以同一个实例既要发布又要接收时，对端必须是能同时接受两种角色的中继/网关；只使用单一角色的实例只建立一条连接。

## 能力

| 能力 | 值 | 说明 |
|---|---|---|
| `ack` | false | 无确认；`ack()` 抛 `UnsupportedFeatureException` |
| `requeue` | false | 无重投；`reject()` 抛 `UnsupportedFeatureException` |
| `delay` | false | `Message::$delayMs` 非空时抛 `UnsupportedFeatureException` |
| `consumerGroups` | false | PUSH 由零端在已连接的 PULL 之间轮询，无消费者组语义 |
| `batchPublish` | false | `publishBatch()` 抛 `UnsupportedFeatureException` |
| `ordering` | true | 单连接内按写入顺序投递；不同连接之间不保证 |
| `headers` | false | ZMTP/NULL 无消息头；非空 headers 抛 `UnsupportedFeatureException` |
| `requiresActiveSubscription` | `pubsub` 为 true，`pushpull` 为 false | PUB/SUB 只向发布前已建立的订阅投递 |

## 协议范围

- 握手：发送 64 字节 greeting（`0xFF` + 8 字节填充 + `0x7F` + 版本 `3.1` + 20 字节 `NULL` + as-server `0` + 31 字节填充），读取并校验对端 greeting；随后交换 `READY` 命令帧，校验 `Socket-Type` 是否兼容（PUB↔SUB、PUSH↔PULL）。
- 帧：1 字节 flags（MORE `0x01`、LONG `0x02`、COMMAND `0x04`）+ 1 或 8 字节大端长度 + body。带 MORE 的连续帧重组为一条消息；body 二进制安全，支持零长度和超过 255 字节（自动使用 LONG）。
- PUB/SUB：SUB 为每个 destination 发送订阅帧 `0x01` + topic 前缀；发布时线上 body 为 `destination . body`，接收时按最长匹配的已订阅前缀剥离。PUB/SUB 过滤由对端（PUB socket）完成，驱动不做客户端过滤；收到不匹配任何订阅的消息抛 `ProtocolException`。
- PUSH/PULL：没有 topic，destination 不写入线上，body 原样发送；PUSH 端由对端在 PULL 连接之间负载均衡。
- `receive()` 的 `timeoutMs` 为 0 时一直阻塞，超时返回 `null`；握手超时抛 `TimeoutException`，连接失败/对端关闭抛 `ConnectionException`，畸形 greeting/帧/元数据抛 `ProtocolException`。
- 不为每条消息生成 id；`PublishResult::$id` 为 null。

## 语义与限制

- **至多一次**：消息只存在于 TCP 缓冲区，订阅者离线、慢消费或对端重启都会丢失消息，没有重放。
- **有界缓冲**：`receive()` 会为其他已订阅 destination 暂存消息，每个 destination 最多 `buffer_limit` 条，超限抛 `ProtocolException`。缓冲仅在内存中，`close()` 或进程退出后丢失，不会持久化。
- **慢加入（slow joiner）**：SUB 的订阅需要先到达 PUB 端；先发布后订阅会丢消息。请使用独立的订阅进程并先完成首次 `receive()`（或短 timeout 调用）再发布。
- **topic 是消息前缀**：这是 ZeroMQ PUB/SUB 的原生过滤语义。第三方发布者必须把 topic 放在 body 开头；`receive()` 会剥离子服务最长匹配的订阅前缀作为消息 body。若两个 destination 存在前缀关系（如 `orders` 与 `orders.eu`），按最长匹配路由。
- **同一实例双连接**：因为一个 ZeroMQ socket 只有一种角色，同时发布和接收会向同一 host:port 建立两条独立连接。多数 libzmq 端点无法在同一端口同时提供 PUB 与 SUB（或 PUSH 与 PULL），生产环境请让发布端、订阅端分别指向对端，或部署一个双角色中继/网关。
- 不支持 bind/listen（驱动只作为连接方）、TLS、CURVE/PLAIN 机制、READY 之后的心跳/命令帧、断线重连和 socket monitor；需要这些能力时请使用 ext-zmq 或代理。
- headers、key、delay、批量发布均不支持且不会静默丢弃：调用会抛 `UnsupportedFeatureException`。
- `ReceivedMessage` 的 `id()` / `key()` 为 null、`attempts()` 恒为 1；`raw()` 包含 `destination`（PUB/SUB）与 `frames`（原始帧列表）。

## 测试

```bash
vendor/bin/phpunit --testsuite unit --filter ZeroMq
vendor/bin/phpunit --testsuite integration --filter ZeroMq
```

协议测试使用 `tests/Protocol/ZeroMq/fixtures/fake_zmtp_peer.php`（纯 PHP 脚本化对端），不需要 pyzmq。集成测试读取：

- `MESSAGING_ZEROMQ_HOST`（默认 `127.0.0.1`）
- `MESSAGING_ZEROMQ_PORT`（默认 `5555`）
- `MESSAGING_ZEROMQ_PATTERN`（默认 `pubsub`）

ZeroMQ 没有 docker-compose 对端；没有进程监听该地址时集成测试自动 skip。对端需能按配置提供相应角色（只发布时为其绑定 SUB，只订阅时为其绑定 PUB，二者都要时为双角色中继）。
