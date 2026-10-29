# Yii3 集成

包内提供 `config/params.php` 与 `config/di.php`，由 `yiisoft/config` 自动合并：`Manager`
注册为容器服务，构造函数注入即可使用。配置键与 `new Manager([...])` 一致。

## 安装

```bash
composer config allow-plugins.erikwang2013/messaging-php true
composer require erikwang2013/messaging-php
```

Yii3 遵循 `yiisoft/config` 的自动合并规则，**不需要复制配置文件**；Composer Plugin 不会为 Yii3 复制配置。应用需要 `yiisoft/config`（标准 Yii3 应用模板默认包含）。

插件通过 `extra.plugin-optional` 声明为可选，但该声明只有 Composer 2.5.3+ 支持：在 2.5.3+ 上未配置 `allow-plugins` 时会跳过插件并继续安装，Composer 2.2–2.5.2 则会安装失败；请保留上面的 `allow-plugins` 命令。Yii3 不依赖该插件的复制动作，插件被跳过不影响配置合并。

在应用 `composer.json` 的 `extra` 中加入（已有这些键时合并到现有数组，不要重复定义）：

```json
{
    "extra": {
        "config-plugin-options": {
            "source-directory": "config"
        },
        "config-plugin": {
            "params": "params.php",
            "di": "di.php"
        }
    }
}
```

## 默认配置

`config/params.php` 提供单连接默认值，读取环境变量：

| 变量 | 默认 | 说明 |
|---|---|---|
| `MESSAGING_DRIVER` | `beanstalkd` | 驱动名 |
| `MESSAGING_HOST` | `127.0.0.1` | broker 地址 |
| `MESSAGING_PORT` | `11300` | broker 端口；换驱动时一起改（redis：6379，nsq：4150，nats：4222） |
| `MESSAGING_TIMEOUT` | `5.0` | 连接超时（秒） |

驱动字段见 [驱动文档](../drivers.md)。

## 覆盖配置

在应用自己的 `config/params.php` 里覆写整个 `erikwang2013/messaging-php` 段：

```php
return [
    'erikwang2013/messaging-php' => [
        'default' => 'primary',
        'connections' => [
            'primary' => [
                'driver' => 'beanstalkd',
                'host' => '10.0.0.10',
                'port' => 11300,
            ],
            'events' => [
                'driver' => 'redis',
                'host' => '10.0.0.11',
                'port' => 6379,
                'group' => 'orders',
            ],
        ],
    ],
];
```

## 使用

```php
use Erikwang2013\Messaging\Manager;

final class OrderService
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

`Manager` 是容器内单例；`Connection` 不注册为服务（它绑定连接名与驱动），按名获取：

```php
$manager->connection('events')->publish('audit', 'logged');
```

消费：

```php
use Erikwang2013\Messaging\Contract\ConsumeOptions;

$manager->connection()->consume('orders', function ($message): void {
    handle($message->body());
    $message->ack();
}, new ConsumeOptions(maxMessages: 100));
```

应用绑定了 `Psr\Log\LoggerInterface` 时自动注入，否则使用 `NullLogger`。

## 注意与限制

- `Connection` 不是容器服务，因为每个实例绑定一个连接名和一个驱动；注入 `Manager` 后按名取用。
- 驱动是同步阻塞 API；常驻进程退出前调用 `$manager->close()`。
- `config/di.php` 里的 `$params` 由 `yiisoft/config` 注入，单独 require 该文件时需要自行提供
  `$params` 变量（包内测试即如此）。
