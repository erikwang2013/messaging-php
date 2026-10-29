# Redis Streams 驱动

驱动名：`redis`。纯 PHP 实现 RESP2 协议，不依赖 phpredis / Predis 扩展。

## 配置

```php
'connections' => [
    'redis' => [
        'driver' => 'redis',
        'host' => '127.0.0.1',
        'port' => 6379,
        'password' => null,
        'database' => 0,
        'group' => 'messaging',
        'consumer' => 'worker-1',
        'max_length' => null,
        'timeout' => 5.0,
    ],
],
```

| 键 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `host` | 是 | — | Redis 主机名或 IP |
| `port` | 否 | `6379` | 端口 |
| `password` | 否 | `null` | 非空时连接后发送 `AUTH` |
| `database` | 否 | `0` | 非 0 时连接后发送 `SELECT` |
| `group` | 否 | `messaging` | 消费者组名 |
| `consumer` | 否 | `<hostname>-<pid>` | 消费者名 |
| `max_length` | 否 | `null` | 设置后 `XADD` 携带 `MAXLEN ~ n`（近似裁剪） |
| `timeout` | 否 | `5.0` | 连接超时（秒） |

目的地（`publish` / `receive` 的第一个参数）即 Redis Stream 的 key。

## 能力

| 能力 | 值 | 说明 |
|---|---|---|
| ack | ✓ | `XACK` |
| requeue | ✗ | 见下方限制 |
| delay | ✗ | 消息带 `delayMs` 时抛 `UnsupportedFeatureException` |
| consumerGroups | ✓ | `XGROUP` / `XREADGROUP` |
| batchPublish | ✓ | 逐条 `XADD` |
| ordering | ✓ | Stream 内按 ID 有序 |
| headers | ✓ | JSON 存于 `headers` 字段 |

## 行为

- 首次接收某个 stream 时执行
  `XGROUP CREATE <stream> <group> 0 MKSTREAM`；从 `0` 开始，首次消费会包含此前积压的消息，
  `BUSYGROUP` 视为成功。发布本身不创建消费者组。
- `publish` 使用 `XADD <stream> [MAXLEN ~ n] * body <body> [headers <json>] [key <key>]`，
  返回的 stream ID 即 `PublishResult::id`。
- `receive` 使用
  `XREADGROUP GROUP <group> <consumer> COUNT 1 BLOCK <timeoutMs> STREAMS <stream> >`。
  `timeoutMs = 0` 时 `BLOCK 0`（服务端无限等待）；超时返回 `null`。
- `ack()` 执行 `XACK`；`reject(false)` 同样执行 `XACK`（丢弃）。
- 消息 `attempts()` 恒为 `1`：`XREADGROUP` 不暴露投递次数。

## 限制

- `reject(true)` 抛 `UnsupportedFeatureException`。Redis Streams 无法同步重新入队；
  未 `XACK` 的消息留在 PEL（Pending Entries List）中。恢复策略由使用方实现：
  用 `XAUTOCLAIM`（Redis 6.2+）或 `XPENDING` + `XCLAIM` 把超时未确认的消息转移给其他消费者。
- 无延迟消息、无 TLS 配置项（v1 仅明文 TCP）。
- 每次 `receive` 最多取一条消息（`COUNT 1`）。
- `max_length` 使用 `MAXLEN ~ n` 近似裁剪：Redis 按内部宏节点整块裁剪，
  小流可能暂时保留多于 `n` 条，属预期行为；需要精确上限请改用 `MAXLEN n` 语义。
- 连接断开后不自动重连。

## 示例

```php
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Manager;

$mq = new Manager([
    'default' => 'redis',
    'connections' => [
        'redis' => ['driver' => 'redis', 'host' => '127.0.0.1', 'group' => 'orders'],
    ],
]);

$mq->extend('redis', new Erikwang2013\Messaging\Driver\Redis\RedisStreamsFactory());

$connection = $mq->connection();
$connection->publish('orders', Message::json(['id' => 1], ['trace' => 'abc']));

while ($message = $connection->receive('orders', new ReceiveOptions(5000))) {
    handle($message->body());
    $message->ack();
}
```

## 测试

```bash
vendor/bin/phpunit --testsuite unit --filter Redis
docker compose --profile redis up -d
vendor/bin/phpunit --group integration --filter RedisStreams
```
