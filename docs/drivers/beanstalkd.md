# Beanstalkd 驱动

纯 PHP 实现的 Beanstalkd 文本协议客户端。目标（destination）即 tube 名。

## 配置

| 键 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `host` | string | 必填 | 服务器地址 |
| `port` | int | `11300` | 端口 |
| `priority` | int | `1024` | `put` 使用的优先级（0 最高） |
| `ttr` | int | `60` | time-to-run，秒 |
| `timeout` | float | `5.0` | TCP 连接超时与普通读写超时，秒 |

```php
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Beanstalkd\BeanstalkdFactory;
use Erikwang2013\Messaging\Manager;

$manager = new Manager([
    'default' => 'queue',
    'connections' => [
        'queue' => ['driver' => 'beanstalkd', 'host' => '127.0.0.1', 'port' => 11300],
    ],
]);
$manager->extend('beanstalkd', new BeanstalkdFactory());

$connection = $manager->connection();
$connection->publish('orders', new Message('payload'));

$message = $connection->receive('orders', new ReceiveOptions(5000));
$message?->ack();
```

## 能力

| 能力 | 支持 | 说明 |
|---|---|---|
| ack | ✓ | `delete` |
| requeue | ✓ | `reject(true)` 用 `release` 保留原优先级、延迟 0；`reject(false)` 等同 `delete` |
| delay | ✓ | `Message::delayMs` 向上取整为秒 |
| consumer groups | ✗ | 同一 tube 竞争消费 |
| batch publish | ✗ | 抛 `UnsupportedFeatureException` |
| ordering | ✗ | 按优先级/延迟调度，不保证 FIFO |
| headers | ✗ | 非空 headers 抛 `UnsupportedFeatureException` |

`key` 同样不支持：设置后 publish 抛 `UnsupportedFeatureException`，不会被静默丢弃。

## 协议范围

命令：`use`、`watch`、`ignore`、`put`、`reserve-with-timeout`、`delete`、`release`、`stats-job`。协议本身不提供 TLS 与认证。

- body 按原始字节写入，长度前缀取自 `strlen`，二进制安全。
- 接收超时协议只支持整秒：`timeoutMs = 0` 发送 `reserve-with-timeout 0`（永久阻塞）；`timeoutMs > 0` 发送 `max(1, ceil(ms / 1000))`，因此 10ms 实际等待约 1 秒。`TIMED_OUT` 与 `DEADLINE_SOON` 都返回 `null`。
- `reject(true)` 先 `stats-job <id>` 读取原始优先级再 `release`；拿不到时回退到连接配置的 `priority`。
- `attempts()` 固定为 1，不额外请求 `stats-job`。
- tube 名在发送前按 Beanstalkd 规则校验（1–200 字节的字母、数字与 `-+/;.$_()`），避免命令注入。
- 连接惰性建立；`close()` 后再操作会重新连接。

## 错误映射

| 情况 | 异常 |
|---|---|
| TCP 连接/读写失败 | `ConnectionException` |
| 套接字读超时 | `TimeoutException` |
| 响应无法解析（空行、缺字段、body 无 CRLF 结尾、未知状态） | `ProtocolException` |
| broker 拒绝请求（`NOT_FOUND`、`JOB_TOO_BIG`、`DRAINING`、`BURIED` 等） | `RequestException` |

## 限制

- 不支持 headers、key、批量发布，能力声明与行为一致。
- 不做自动重连与连接池；每个连接同一时刻只应被一个进程使用。
- Beanstalkd 在内存不足时对 `put` 返回 `BURIED <id>`，本驱动按请求失败抛 `RequestException`（不静默接受）。

## 集成测试

```bash
docker compose --profile beanstalkd up -d
vendor/bin/phpunit --group integration --filter Beanstalkd

# 自定义地址
MESSAGING_BEANSTALKD_HOST=127.0.0.1 MESSAGING_BEANSTALKD_PORT=11300 \
  vendor/bin/phpunit --group integration --filter Beanstalkd
```
