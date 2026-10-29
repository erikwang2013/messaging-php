# RabbitMQ 驱动

纯 PHP 实现的 AMQP 0-9-1 客户端，直接基于 TCP/TLS 收发协议帧，不依赖 `ext-amqp` 或 `php-amqplib`。目标（destination）即队列名，消息通过默认交换机发布。

## 配置

| 键 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `host` | string | 必填 | 服务器地址 |
| `port` | int | `5672`（`tls=true` 时 `5671`） | AMQP 端口 |
| `username` | string | `guest` | SASL PLAIN 用户名 |
| `password` | string | `guest` | SASL PLAIN 密码 |
| `vhost` | string | `/` | 虚拟主机 |
| `heartbeat` | int | `60` | 心跳间隔秒数，`0` 关闭心跳 |
| `timeout` | float | `5.0` | 连接与命令响应超时，秒 |
| `tls` | bool | `false` | 使用 TLS 连接（`tls://`） |
| `tls_context` | array | `[]` | 透传给 `stream_context_create()` 的 SSL 上下文，例如 `['ssl' => ['cafile' => '/path/ca.pem']]` |
| `durable` | bool | `true` | `queue.declare` 的 durable 标志，同时决定发布消息的 `delivery-mode`（durable 时 2） |
| `prefetch` | int | `1` | `basic.qos` 的 prefetch count，`0` 表示不限 |

```php
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\RabbitMq\RabbitMqFactory;
use Erikwang2013\Messaging\Manager;

$manager = new Manager([
    'default' => 'queue',
    'connections' => [
        'queue' => [
            'driver' => 'rabbitmq',
            'host' => '127.0.0.1',
            'port' => 5672,
            'username' => 'guest',
            'password' => 'guest',
            'vhost' => '/',
        ],
    ],
]);
$manager->extend('rabbitmq', new RabbitMqFactory());

$connection = $manager->connection();
$connection->publish('orders', new Message('payload', ['trace' => 'abc'], 'order-1'));

$message = $connection->receive('orders', new ReceiveOptions(5000));
if ($message !== null) {
    try {
        handle($message->body(), $message->headers());
        $message->ack();
    } catch (Throwable $e) {
        $message->reject(true);
    }
}
```

## 能力

| 能力 | 支持 | 说明 |
|---|---|---|
| ack | ✓ | `basic.ack` |
| requeue | ✓ | `reject(true)` 发送 `basic.nack requeue=true`；`reject(false)` 发送 `basic.nack requeue=false` |
| delay | ✗ | 需要 `rabbitmq_delayed_message_exchange` 插件，本驱动未实现；`Message::delayMs` 非空时抛 `UnsupportedFeatureException` |
| consumer groups | ✗ | 同一队列上的消费者天然竞争消费 |
| batch publish | ✗ | 抛 `UnsupportedFeatureException` |
| ordering | ✗ | 单队列通常 FIFO，但协议不承诺 |
| headers | ✓ | 通过 `application_headers` 字段表原样往返 |

## 语义

- **目标即队列**：`publish()` 与 `receive()` 都会幂等执行 `queue.declare`（durable 由配置决定），发布到默认交换机 `''`，routing key 等于队列名。
- **消息 ID**：发布时生成 32 位十六进制 ID 写入 `message-id`，`PublishResult::$id` 与接收到的 `ReceivedMessage::id()` 一致。其他客户端未设置 `message-id` 时，`id()` 为 `null`。
- **key**：`Message::$key` 写入 `correlation-id`；接收时优先取 `correlation-id`，缺失时回退为 delivery 的 routing key。
- **headers**：支持 string、int（自动选择 32/64 位）、float、bool、null、嵌套表与数组。其他类型（对象、资源）在发布时抛 `InvalidArgumentException`，不会静默丢弃。
- **接收**：首次 `receive()` 对目标队列发起 `basic.consume`（`no-ack=false`），后续投递在驱动内缓冲，按到达顺序返回。`timeoutMs=0` 永久阻塞；`timeoutMs>0` 到期返回 `null`。服务器要求的心跳帧在等待期间自动应答，客户端也按协商间隔发送心跳。
- **确认**：`ack()` 使用投递时的 delivery tag 发送 `basic.ack`；`attempts()` 固定为 1，不读取 `x-death` 等重投计数。
- **连接**：工厂在 `Manager::connection()` 时立即完成握手并应用 `basic.qos`，认证或端点错误当场抛出。连接惰性复用，不做自动重连。
- **关闭**：`close()` 发送 `connection.close`（200）后关闭套接字；已声明的队列保留。

## 协议范围

- 协议头 `AMQP\x00\x00\x09\x01`；SASL PLAIN。
- 帧类型：method（1）、header（2）、body（3）、heartbeat（8），帧尾 `0xCE`；短字符串、长字符串、字段表、bit、大端整数编解码。
- 方法：`connection.start/start-ok`、`tune/tune-ok`、`open/open-ok`、`close/close-ok`、`blocked/unblocked`；`channel.open/open-ok/close/close-ok`；`queue.declare/declare-ok`；`basic.qos/qos-ok`、`basic.consume/consume-ok/cancel`、`basic.publish`、`basic.deliver`、`basic.ack`、`basic.nack`；`basic.return` 与 publisher confirm 的 `basic.ack/nack` 会被识别但不启用。
- 内容帧按协商的 `frame-max` 自动分片；body 二进制安全。
- 心跳按 AMQP 规则协商：任一方为 0 即关闭，否则取二者最小值；空闲时发送心跳并忽略收到的空心跳帧。
- TLS 使用 `tls://` 流，端口默认 5671，`tls_context` 透传 SSL 选项。

## 错误映射

| 情况 | 异常 |
|---|---|
| TCP 连接/读写失败、无法轮询套接字 | `ConnectionException` |
| 等待命令响应或帧超时 | `TimeoutException` |
| `connection.close` 403（认证/权限拒绝） | `AuthenticationException` |
| 其他 `connection.close` | `ConnectionException` |
| `channel.close`（404、406 等） | `ProtocolException` |
| 帧格式错误、未知方法/字段类型、内容长度不符 | `ProtocolException` |
| 不支持的延迟、批量发布 | `UnsupportedFeatureException` |

## 限制

- 只使用默认交换机与队列名路由：不支持自定义 exchange、binding、topic/fanout 路由与 `basic.get`。
- 未启用 publisher confirm；`basic.publish` 写入套接字即返回。队列已声明，因此默认交换机路由存在，但 broker 侧异步失败不会反馈给发布者。
- 单连接单 channel，不做重连、连接池或并发消费；同一驱动实例同一时刻应只被一个进程/协程使用。
- `prefetch` 限制未确认消息数；超过后 broker 暂停投递，直到 ack/nack。
- 队列不会自动删除；集成测试使用随机队列名避免冲突。
- 心跳/帧超时依赖 `stream_select`，不支持非阻塞/Fiber API。

## 集成测试

```bash
docker compose --profile rabbitmq up -d
vendor/bin/phpunit --group integration --filter RabbitMq

# 自定义地址
MESSAGING_RABBITMQ_HOST=127.0.0.1 MESSAGING_RABBITMQ_PORT=5672 \
MESSAGING_RABBITMQ_USER=guest MESSAGING_RABBITMQ_PASS=guest MESSAGING_RABBITMQ_VHOST=/ \
  vendor/bin/phpunit --group integration --filter RabbitMq
```

broker 不可达时集成测试自动 `markTestSkipped`。协议层测试使用 `tests/Protocol/RabbitMq/fake_amqp_broker.php`，无需 Docker。
