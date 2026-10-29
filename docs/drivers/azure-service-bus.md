# Azure Service Bus 驱动

驱动名：`azure-service-bus`。纯 PHP 实现 Azure Service Bus **data-plane REST** 接口，
使用 Shared Access Signature（SAS）认证，不依赖 `azure-service-bus` SDK 或 AMQP 扩展。
HTTP 使用 Guzzle，HMAC 使用 `hash_hmac`（ext-hash，PHP 默认启用）。

- 工厂类：`Erikwang2013\Messaging\Driver\Azure\ServiceBusFactory`
- 驱动类：`Erikwang2013\Messaging\Driver\Azure\ServiceBusDriver`
- SAS 签名：`Erikwang2013\Messaging\Driver\Azure\SasToken`

> 官方文档（实现依据，2026-10 核对）：
> [Send Message](https://learn.microsoft.com/en-us/rest/api/servicebus/send-message-to-queue)、
> [Peek-Lock Message](https://learn.microsoft.com/en-us/rest/api/servicebus/peek-lock-message-non-destructive-read)、
> [Delete Message（complete）](https://learn.microsoft.com/en-us/rest/api/servicebus/delete-message)、
> [Unlock Message（abandon）](https://learn.microsoft.com/en-us/rest/api/servicebus/unlock-message)、
> [SAS token 生成](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-sas#generation-of-a-sas-token)、
> [HTTP message headers and properties](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-headers-and-properties)。

## 配置

```php
use Erikwang2013\Messaging\Driver\Azure\ServiceBusFactory;
use Erikwang2013\Messaging\Manager;

$mq = new Manager([
    'default' => 'azure',
    'connections' => [
        'azure' => [
            'driver' => 'azure-service-bus',
            'namespace' => 'my-namespace',            // 与 endpoint 二选一
            // 'endpoint' => 'https://my-namespace.servicebus.windows.net',
            'queue' => 'orders',                       // queue 或 topic+subscription 二选一
            // 'topic' => 'events',
            // 'subscription' => 'workers',
            'shared_access_key_name' => 'RootManageSharedAccessKey',
            'shared_access_key' => '...base64 key from the portal...',
            'timeout' => 30.0,
        ],
    ],
]);
$mq->extend('azure-service-bus', new ServiceBusFactory());
```

| 键 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `namespace` | 与 `endpoint` 二选一 | — | 生成 `https://{namespace}.servicebus.windows.net` |
| `endpoint` | 与 `namespace` 二选一 | — | 显式 API 根地址；允许 `http://`（仅测试/代理），主机名会转小写 |
| `queue` | 与 `topic` 二选一 | — | 默认队列；发布和接收都使用它 |
| `topic` | 与 `queue` 二选一 | — | 与 `subscription` 同时提供；发布到 topic，从 subscription 接收 |
| `subscription` | 与 `topic` 成对 | — | 订阅名 |
| `shared_access_key_name` | 是 | — | 授权规则名（Policies → Shared access policies） |
| `shared_access_key` | 是 | — | 授权规则的 primary/secondary key，按字符串使用，**不要 base64 解码** |
| `timeout` | 否 | `30.0` | 普通 HTTP 超时（秒）；也是 `timeoutMs = 0` 时的默认接收等待时间 |
| `client` | 否 | — | 注入 `GuzzleHttp\ClientInterface`（测试 / DI 用） |

`destination` 参数优先于连接级默认值：

- 发布：传队列名或 topic 名；`''` 使用连接配置。传 `topic/subscriptions/sub` 会抛 `InvalidArgumentException`。
- 接收：传队列名，或完整订阅路径 `topic/subscriptions/sub`；`''` 使用连接配置
  （topic + subscription 模式会拼成 `topic/subscriptions/sub`）。
- 实体名只允许字母、数字、`.`、`_`、`~`、`-`，且以字母或数字开头。

驱动**不创建**队列、topic 或 subscription，也不修改其配置，必须事先在 Azure 侧存在。

## 示例

```php
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;

$conn = $mq->connection('azure');

$conn->publish('orders', Message::json(['id' => 1], ['trace' => 'abc'], 'customer-42'));

while ($message = $conn->receive('orders', new ReceiveOptions(5000))) {
    try {
        handle($message->body(), $message->headers());
        $message->ack();              // peek-lock + Delete Message
    } catch (Throwable $e) {
        $message->reject(true);       // Unlock Message，立即可重投
    }
}
```

延迟发布：

```php
$conn->publish('orders', new Message('retry', [], null, 30_000)); // 30 秒后可见
```

## 认证（SAS）

```
SharedAccessSignature sr={urlencode(resourceURI)}&sig={urlencode(base64(HMAC-SHA256(key, urlencode(resourceURI) + "\n" + expiry)))}&se={expiry}&skn={urlencode(keyName)}
```

- `resourceURI` 为 `{endpoint}/{实体路径}`（队列，或 topic 用于发布）；先转小写，再做 URL 编码后参与签名，
  `sr=` 也使用同一小写形式（Azure 按小写规范校验签名）。
- `expiry` 为 UNIX 秒；token 有效期 3600 秒，剩余 300 秒时自动刷新。
- 每个实体路径单独缓存 token；所有请求携带 `Authorization: SharedAccessSignature ...`。
- key 按 Azure 门户字符串原样参与 HMAC，不做 base64 解码。
- 支持命名空间级或实体级授权规则；订阅沿用 topic/namespace 的规则（Service Bus 不支持订阅级 SAS 规则）。

## 请求映射

所有消息操作都不需要 `api-version` 查询参数（官方文档明确说明 BrokerProperties 映射与 `api-version` 无关）。

| 操作 | HTTP | 路径 |
|---|---|---|
| 发布 | `POST` | `{endpoint}/{queue|topic}/messages` |
| peek-lock 接收 | `POST` | `{endpoint}/{queue|topic/subscriptions/sub}/messages/head?timeout={seconds}` |
| ack（complete） | `DELETE` | peek-lock 响应 `Location` 头原样地址 |
| reject(true)（abandon） | `PUT` | peek-lock 响应 `Location` 头原样地址 |
| reject(false)（丢弃） | `DELETE` | 同上，等价 complete |

- 发布：`Content-Type: text/plain`（可用 `ContentType` / `Content-Type` 头覆盖），
  `BrokerProperties` 头为 JSON；自定义属性作为独立 HTTP 头。请求体为原始字节。
- 接收：`201` 返回消息和 `BrokerProperties`（含 `MessageId`、`LockToken`、`DeliveryCount`、
  `SequenceNumber` 等）；`204` 表示等待超时，`receive()` 返回 `null`。
  `timeoutMs` 向上取整为秒，最小 1 秒；`timeoutMs = 0` 使用配置的 `timeout`（默认 30 秒）。
- 驱动优先使用响应 `Location` 头完成 settle；缺失时才用 `MessageId/SequenceNumber` + `LockToken` 拼路径。

### BrokerProperties 与 headers

| 来源 | 去向 |
|---|---|
| `Message::key` | `BrokerProperties.CorrelationId`；接收时反向映射为 `key()` |
| `Message::delayMs` | `BrokerProperties.ScheduledEnqueueTimeUtc = now + delayMs`（秒级精度） |
| `Label` / `CorrelationId` / `ReplyTo` / `ReplyToSessionId` / `To` / `SessionId` / `PartitionKey` | 对应 BrokerProperties（字符串） |
| `TimeToLive` | `BrokerProperties.TimeToLive`（正整数秒） |
| `ScheduledEnqueueTimeUtc` | `BrokerProperties.ScheduledEnqueueTimeUtc`（HTTP-date、UNIX 时间戳或 `DateTimeInterface`） |
| `MessageId` | 覆盖驱动生成的 32 位十六进制 id |
| `ContentType` / `Content-Type` | HTTP `Content-Type`（不出现在接收端 headers） |
| 其他 header | 自定义消息属性，逐个头传输 |

- 自定义 header 值支持 string / int / float / bool。字符串会加引号发送（避免 Service Bus 把
  `"123"` 推断为整数）；接收时按官方类型规则还原。
- `Authorization`、`BrokerProperties`、`x-ms-*`、`Host`、`Content-Length` 等保留头会抛
  `InvalidArgumentException`，**不会静默丢弃**。数组/对象值同样抛异常。
- `Message::key` 与 `CorrelationId` 头同时出现且值不同会抛异常。
- 接收端 `headers()` 只包含自定义属性；系统属性在 `raw()['broker_properties']`，
  `Location`/`Content-Type`/状态码在 `raw()` 的其他键中。
- `raw()` 结构：`['broker_properties' => array, 'location' => string, 'status' => int, 'content_type' => string]`。

## 能力

| 能力 | 值 | 说明 |
|---|---|---|
| ack | ✓ | `DELETE`（Delete Message / complete） |
| requeue | ✓ | `PUT`（Unlock Message）；立即可重投，`DeliveryCount` 递增 |
| delay | ✓ | 发布时 `ScheduledEnqueueTimeUtc`（秒级精度） |
| consumer group | ✗ | 通过 topic + subscription 在 broker 侧配置，驱动不管理 |
| batch publish | ✗ | 仅单条发送；`publishBatch()` 抛 `UnsupportedFeatureException` |
| ordering | ✗ | 不实现 session；`SessionId` 头仅透传，不保证 FIFO |
| headers | ✓ | 自定义属性 + 常用 BrokerProperties |
| requiresActiveSubscription | ✗ | 队列/订阅需预先存在，但无需先调用 `receive()` |

## 限制

- **仅 REST + SAS**：不支持 AMQP、Microsoft Entra ID（OAuth）认证、事务、send-via、会话（FIFO）、
  重复检测管理、主题规则管理、订阅自动创建。
- **不做自动重试**：429/5xx 直接抛 `RequestException`；重复投递由 Service Bus 至少一次语义决定，
  消费端需幂等。
- **peek-lock 锁期**：默认锁期由实体配置决定；长任务需在锁过期前完成，或自行调用
  Renew-Lock（本驱动未封装）。锁过期后 complete/abandon 会返回 404。
- **`reject(false)` 是丢弃（complete）**，不是 dead-letter。REST data-plane 没有 dead-letter 操作；
  需要 DLQ 时请依赖 `MaxDeliveryCount` 或 Azure 侧策略。
- **延迟精度为秒**：`delayMs` 向上取整；`reject(true)` 不支持附带冷却时间，立即重投。
- **批量发送未实现**（REST 有 Send Message Batch，本驱动未封装）。
- **接收是同步 HTTP 长轮询**：`timeoutMs = 0` 时单次最多等待配置的 `timeout`（默认 30 秒），
  返回 `null` 后由 `consume()` 循环；高吞吐场景建议多进程并行。
- **Service Bus emulator 不支持 REST data-plane**，因此集成测试默认使用内存假传输；
  真实 Azure 测试为可选，且队列必须预建。
- 实体名大小写不敏感；请求路径保留调用方传入的大小写，但 SAS 始终用小写规范化的资源 URI 签名。
- 自定义 header 名按 HTTP 语义大小写不敏感；返回时保留服务端返回的写法。

## 错误映射

| HTTP / 异常 | 抛出的异常 |
|---|---|
| 401 / 403 | `AuthenticationException` |
| 404（send/receive） | `RequestException`，消息含 queue/topic not found |
| 404（complete/abandon） | `RequestException`，消息含 `lock lost` |
| 410 | `RequestException`，消息含 queue/subscription does not exist |
| 其他非 2xx | `RequestException`（`statusCode()` + `responseBody()`） |
| 连接失败 | `ConnectionException`；超时为 `TimeoutException` |
| 响应缺少/非法 `BrokerProperties` | `ProtocolException` |

## 测试

```bash
vendor/bin/phpunit --testsuite unit --filter 'Azure|SasToken'
vendor/bin/phpunit --testsuite integration --filter ServiceBusTest
vendor/bin/phpstan analyse src/Driver/Azure --level=5
vendor/bin/php-cs-fixer fix --dry-run --diff --config=.php-cs-fixer.dist.php src/Driver/Azure tests/Unit/Driver/Azure tests/Protocol/Azure tests/Integration/ServiceBusTest.php
```

- 单元测试：SAS 已知向量与缓存、请求构造（send/receive/complete/abandon/schedule）、
  BrokerProperties 解析、自定义 header 类型、能力、错误映射（401/404/410）、配置校验。
- 协议测试：`tests/Protocol/Azure/fixtures/fake_servicebus_server.php` 起本地 HTTP 假 broker，
  覆盖 send / peek-lock / complete / abandon / delay / 401 / 404 / 410，并校验 SAS 签名。
- 集成测试：共享契约测试跑内存传输；真实 Azure 可选：

```bash
MESSAGING_AZURE_SERVICEBUS_NAMESPACE=my-namespace \
MESSAGING_AZURE_SERVICEBUS_KEY_NAME=RootManageSharedAccessKey \
MESSAGING_AZURE_SERVICEBUS_KEY='...' \
MESSAGING_AZURE_SERVICEBUS_QUEUE=orders \
vendor/bin/phpunit --testsuite integration --filter ServiceBusTest
```

缺少上述任一环境变量时 live 测试自动 skip。
