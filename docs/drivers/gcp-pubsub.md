# Google Cloud Pub/Sub 驱动

驱动名：`gcp-pubsub`。纯 PHP 实现 Google Cloud Pub/Sub v1 REST JSON API，服务账号 JWT（RS256）认证，
不依赖 `google/cloud-pubsub`。HTTP 使用 Guzzle，签名使用 ext-openssl。

- 工厂类：`Erikwang2013\Messaging\Driver\Gcp\GcpPubSubFactory`
- 驱动类：`Erikwang2013\Messaging\Driver\Gcp\GcpPubSubDriver`

## 配置

```php
'connections' => [
    'pubsub' => [
        'driver' => 'gcp-pubsub',
        'project_id' => 'my-project',                  // 必填
        'credentials_file' => '/etc/gcp/sa.json',      // 与 credentials_json 二选一
        // 'credentials_json' => '{...service account JSON...}',
        'subscription' => 'orders-default',            // 可选：receive('') 的默认订阅
        'ack_deadline_seconds' => 60,                  // 可选：拉取后延长租约（秒），0 关闭
        'endpoint' => 'https://pubsub.googleapis.com', // 可选
        'timeout' => 30.0,                             // 可选：普通请求超时（秒）
    ],
],
```

| 键 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `project_id` | 是 | — | GCP 项目 ID |
| `credentials_file` | 二选一 | — | 服务账号 JSON 文件路径 |
| `credentials_json` | 二选一 | — | 服务账号 JSON 字符串（也接受已解析的数组） |
| `emulator_host` | 否 | `null` | 如 `127.0.0.1:8085`；设置后使用 `http://` 且不认证 |
| `endpoint` | 否 | `https://pubsub.googleapis.com` | API 根地址；显式设置时优先于 emulator 推导值 |
| `subscription` | 否 | `null` | `receive('')` 时使用的订阅名 |
| `ack_deadline_seconds` | 否 | `60` | 拉取到消息后调用 `:modifyAckDeadline` 延长租约的秒数，整数 0–600；`0` 关闭这次额外请求 |
| `timeout` | 否 | `30.0` | 发布、ack、租约续期、token 交换的 HTTP 超时（秒）；`receive()` 的 pull 请求使用调用方超时 |

`publish()` 的 `destination` 是 **topic**，`receive()` 的 `destination` 是 **subscription**。
Pub/Sub 不要求两者同名，但集成测试使用同名资源以便复用契约测试。
驱动不会创建 topic/subscription，二者必须事先存在。

## 示例

```php
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Gcp\GcpPubSubFactory;
use Erikwang2013\Messaging\Manager;

$mq = new Manager([
    'default' => 'gcp',
    'connections' => [
        'gcp' => [
            'driver' => 'gcp-pubsub',
            'project_id' => 'my-project',
            'credentials_file' => '/etc/gcp/sa.json',
        ],
    ],
]);
$mq->extend('gcp-pubsub', new GcpPubSubFactory());

$conn = $mq->connection();
$conn->publish('orders', Message::json(['id' => 1], ['trace' => 'abc'], 'customer-42'));

while ($message = $conn->receive('orders-subscription', new ReceiveOptions(5000))) {
    handle($message->body(), $message->headers());
    $message->ack();
}
```

## 认证

- 从服务账号 JSON 读取 `client_email`、`private_key`、`private_key_id`。
- 构造 JWT：`{iss, scope: https://www.googleapis.com/auth/pubsub, aud: https://oauth2.googleapis.com/token, iat, exp}`，
  `exp = iat + 3600`，用 `OPENSSL_ALGO_SHA256` 做 RS256 签名。
- 以 `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer` 调用 `https://oauth2.googleapis.com/token`
  换取 access token，并缓存到 `expires_in - 60s`；到期前 60 秒自动刷新。
- 所有 Pub/Sub 请求携带 `Authorization: Bearer <token>`。
- `emulator_host` 设置时完全跳过 token 交换（模拟器不校验认证）。

## 能力

| 能力 | 值 | 说明 |
|---|---|---|
| ack | ✓ | `subscriptions/{sub}:acknowledge` |
| requeue | ✓ | `reject(true)` 调 `:modifyAckDeadline` 且 `ackDeadlineSeconds: 0`，立即重新投递；`reject(false)` 等价 ack（丢弃） |
| delay（发布） | ✗ | 消息带 `delayMs` 抛 `UnsupportedFeatureException` |
| consumer group | ✓ | Pub/Sub subscription 天然为多消费者竞争消费 |
| batch publish | ✓ | 一次 HTTP 调用携带全部 `messages`，逐条返回 `messageIds[i]` |
| ordering | ✓ | `Message::$key` 映射为 `orderingKey` |
| headers | ✓ | 映射为 Pub/Sub 字符串 `attributes`；非标量值抛 `UnsupportedFeatureException`，不会静默丢弃 |
| requiresActiveSubscription | ✗ | 无需先调用 `receive()`；但 subscription 必须由外部创建 |

`ReceivedMessage::attempts()` 恒为 `1`：Pub/Sub 只有在配置 dead-letter policy 时才暴露
`deliveryAttempt`，驱动 v1 不管理该策略。

## 行为

- **发布**：`POST /v1/projects/{project}/topics/{topic}:publish`，
  body `{"messages":[{"data": base64(body), "attributes": {...}, "orderingKey": key}]}`。
  `PublishResult::id` 为服务端返回的 `messageIds[i]`。
- **接收**：`POST /v1/projects/{project}/subscriptions/{sub}:pull`，
  body `{"maxMessages":1, "returnImmediately":false}`，返回 `receivedMessages[0]`。
  - 拉取成功后、返回 `ReceivedMessage` 前，若 `ack_deadline_seconds > 0`，先调用
    `:modifyAckDeadline` 把该消息租约延长到配置秒数（默认 60），避免处理时间超过订阅默认
    ack deadline（通常 10 秒）而被重复投递。每次成功拉取因此多一次 HTTP 请求。
  - `timeoutMs > 0`：在该期限内轮询（每次请求以剩余时间作为 HTTP 超时），期限内无消息返回 `null`。
  - `timeoutMs = 0`：循环长轮询（单次 HTTP 超时 95 秒，Pub/Sub 服务端约 90 秒返回空响应），
    直到收到消息；网络错误抛 `ConnectionException`。
- **确认/重投**：`ack()` → `:acknowledge`；`reject(true)` → `:modifyAckDeadline`（0 秒）；
  `reject(false)` → `:acknowledge`。
- **错误映射**：HTTP 401/403 → `AuthenticationException`；其他非 2xx → `RequestException`
  （含 `statusCode()` 与 `responseBody()`）；连接失败 → `ConnectionException`；响应非法 JSON → `ProtocolException`。
- 不做自动重试，避免产生重复消息。

## 限制

- **仅 REST pull，不使用 streaming pull**：每条 `receive()` 最多取一条消息；高吞吐场景建议多进程/多 worker 并行拉取。
- **长任务受租约限制**：消息必须在 `ack_deadline_seconds`（默认 60 秒，最大 600）内处理完并
  ack/reject，否则 Pub/Sub 会重新投递并产生重复。处理更久请调大该值；超过 600 秒需拆分任务或
  自行实现续租（驱动不在处理期间再次续租）。
- **无 exactly-once**：Pub/Sub 默认至少一次投递；重复消息需消费端自行幂等。
- **不管理 dead-letter policy**：订阅的 dead-letter 配置需在 GCP 侧预建，驱动不读取 `deliveryAttempt`。
- **不管理 topic/subscription**：不自动创建、不自动删除；`receive()` 对不存在的订阅返回 `RequestException`（404）。
- **无延迟发布**、无 schema、无 push 订阅、无 seek/snapshot。
- 顺序性依赖 `orderingKey` + 订阅侧开启 message ordering；驱动只负责传递 key，不修改订阅配置。
- header（attribute）值必须可转为字符串；数组/对象会抛异常而不是被丢弃。
- `timeoutMs = 0` 的阻塞拉取会持续循环，直到收到消息或抛连接错误，驱动内部无法取消；
  需要可中断的消费循环时请使用有限超时并在业务层检查退出条件。
- 明文 HTTP 仅用于模拟器；真实服务走 HTTPS。

## 测试

```bash
vendor/bin/phpunit --testsuite unit --filter 'Gcp|ServiceAccountTokenProvider'

docker compose --profile gcp up -d
MESSAGING_PUBSUB_EMULATOR_HOST=127.0.0.1:8085 \
MESSAGING_GCP_PROJECT=test-project \
vendor/bin/phpunit --testsuite integration --filter PubSubTest
```

集成测试在 `setUp()` 中通过 REST 创建 topic 与 subscription（同名随机资源），
`tearDown()` 删除；模拟器不可达时自动 skip。模拟器不校验认证，因此集成测试不覆盖 token 交换；
JWT 构造与 RS256 签名由单元测试用 openssl 公钥验证。
