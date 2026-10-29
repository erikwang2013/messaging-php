# Hyperf 集成

`Manager` 通过 Hyperf 容器按类型注入，配置放在 `config/autoload/messaging.php`，形状与直接
`new Manager([...])` 完全一致。本集成不引入 `hyperf/framework` 依赖，只在装有 Hyperf 的项目里生效。

## 安装

```bash
composer config allow-plugins.erikwang2013/messaging-php true
composer require erikwang2013/messaging-php
```

本包是 Composer Plugin。安装/更新时若检测到 `hyperf/framework`，会自动把默认配置复制到 `config/autoload/messaging.php`（已存在则不覆盖）；`vendor:publish` 仍可用于重新发布。

在应用 `composer.json` 里登记 ConfigProvider（已有 `extra.hyperf.config` 时把类名追加进数组）：

```json
{
    "extra": {
        "hyperf": {
            "config": "Erikwang2013\\Messaging\\Integration\\Hyperf\\ConfigProvider"
        }
    }
}
```

## 发布配置

```bash
php bin/hyperf.php vendor:publish erikwang2013/messaging-php
```

会生成 `config/autoload/messaging.php`：

```php
return [
    'default' => 'default',
    'connections' => [
        'default' => [
            'driver' => 'beanstalkd',
            'host' => '127.0.0.1',
            'port' => 11300,
            'timeout' => 5.0,
        ],
    ],
];
```

字段含义与各驱动文档一致（见 [驱动文档](../drivers.md)）。多连接时继续往 `connections`
里加，连接名供 `$manager->connection('name')` 使用。

## 环境变量

未发布配置时可直接用环境变量（`.env` 同样生效）：

| 变量 | 默认 | 说明 |
|---|---|---|
| `MESSAGING_DRIVER` | `beanstalkd` | 驱动名：`beanstalkd` / `redis` / `nsq` / `nats` / `activemq` / `artemis` |
| `MESSAGING_HOST` | `127.0.0.1` | broker 地址 |
| `MESSAGING_PORT` | `11300` | broker 端口；换驱动时一起改（redis：6379，nsq：4150，nats：4222） |
| `MESSAGING_TIMEOUT` | `5.0` | 连接超时（秒） |

## 使用

```php
use Erikwang2013\Messaging\Manager;

class OrderController
{
    public function __construct(private Manager $messaging)
    {
    }

    public function publish(): void
    {
        $this->messaging->connection()->publish('orders', 'payload');
    }
}
```

消费（建议放在独立 command，见下方限制）：

```php
use Erikwang2013\Messaging\Contract\ConsumeOptions;

$connection = $manager->connection();

$connection->consume('orders', function ($message): void {
    handle($message->body());
    $message->ack();
}, new ConsumeOptions(maxMessages: 100));
```

多连接：

```php
$manager->connection('primary')->publish('orders', 'payload');
$manager->connection('events')->publish('audit', 'logged');
```

## 注意与限制

- `Manager` 由容器管理为单例；连接在第一次 `connection()` 时惰性建立并在 Manager 内缓存。
  常驻进程退出前调用 `$manager->close()` 释放套接字。
- 驱动是同步阻塞 socket 实现。在协程中运行依赖 Swoole 的 hook 行为，消费循环仍会长期占用
  当前协程；生产消费建议用独立 command 进程（`php bin/hyperf.php` 自定义命令）。
- 日志自动接入容器里的 `Psr\Log\LoggerInterface`；没有绑定时使用 `NullLogger`。
- 本包不依赖 `hyperf/framework`：`ManagerFactory` 用字符串取 `Hyperf\Contract\ConfigInterface`，
  未装 Hyperf 时这段代码不会被加载。
