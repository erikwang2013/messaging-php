# messaging-php

纯 PHP 实现的统一消息队列 / 事件流客户端，一套 `publish / receive / ack` API 覆盖 20 种系统：

> RabbitMQ、Kafka、RocketMQ、Pulsar、ActiveMQ、ActiveMQ Artemis、NATS、Redpanda、AutoMQ、Redis Streams、AWS SQS、AWS SNS、AWS Kinesis、Google Cloud Pub/Sub、Azure Service Bus、Azure Event Hubs、ZeroMQ、NSQ、Beanstalkd、Confluent Platform

不依赖 broker SDK，不需要 rdkafka / ext-zmq / ext-redis 等扩展；TCP 协议用 PHP 标准库实现，HTTP 云服务用 Guzzle 直连。

> 当前进度：**Phase 1**。已支持 Beanstalkd、Redis Streams、NSQ、ActiveMQ、ActiveMQ Artemis、NATS Core；其余驱动按 [实施规划](docs/superpowers/plans/2026-10-29-messaging-php.md) 分阶段实现，完成情况见 [驱动支持矩阵](docs/drivers.md)。

## 安装

```bash
composer require erikwang2013/messaging-php
```

要求 PHP >= 8.1，标准扩展 openssl、zlib 即可。

## 快速开始

```php
<?php

use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Manager;

$mq = new Manager([
    'default' => 'redis',
    'connections' => [
        'redis' => [
            'driver' => 'redis',
            'host' => '127.0.0.1',
            'port' => 6379,
            'group' => 'orders',
        ],
    ],
]);

$connection = $mq->connection();

$connection->publish('orders', Message::json(['id' => 1], ['trace' => 'abc']));

while ($message = $connection->receive('orders', new ReceiveOptions(timeoutMs: 5000))) {
    try {
        // 处理消息
        $message->ack();
    } catch (Throwable $e) {
        $message->reject(requeue: true);
    }
}
```

`receive()` 是统一的拉取式消费入口；push 型协议会在驱动内部缓冲。`consume()` 只是循环糖：

```php
$connection->consume('orders', function ($message): void {
    // 处理消息，处理成功后自行 ack()
    $message->ack();
}, new ConsumeOptions(timeoutMs: 5000));
```

## 配置

采用工作区惯用的数组配置：`default` + `connections.<name>.driver`，其余键由各驱动解释。能力差异（ack / requeue / delay / consumer group / batch）通过 `$connection->capabilities()` 查询，驱动不支持时抛 `UnsupportedFeatureException`，不会静默忽略。

## 驱动支持

见 [docs/drivers.md](docs/drivers.md)。支持级别：

- **一等**：纯 PHP 实现，契约测试 + 真实 broker 集成测试
- **实验**：协议实现范围有限，接口可能调整
- **受限**：broker 语义本身缺失（如 ZeroMQ 无持久化/ack、SNS 只能发布）

## 开发

```bash
composer install
composer test          # 单元 + 协议测试，无需外部服务
composer phpstan
```

集成测试按 broker 启动对应容器后运行：

```bash
docker compose --profile redis up -d
composer test-integration -- --filter RedisStreams
```

## 许可证

MIT，见 [LICENSE](LICENSE)。
