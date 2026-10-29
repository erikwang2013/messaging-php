# Laravel 集成

Laravel 适配层只做三件事：合并配置、把 `Erikwang2013\Messaging\Manager` 注册为容器单例、提供 `messaging` 绑定和 `Messaging` Facade。配置结构与原生 `new Manager([...])` 完全一致，适配层不含任何驱动逻辑。

## 安装

```bash
composer require erikwang2013/messaging-php
```

包在 `composer.json` 的 `extra.laravel` 中声明了自动发现信息，Laravel 会据此注册 provider 与 alias：

```json
{
    "extra": {
        "laravel": {
            "providers": [
                "Erikwang2013\\Messaging\\Integration\\Laravel\\MessagingServiceProvider"
            ],
            "aliases": {
                "Messaging": "Erikwang2013\\Messaging\\Integration\\Laravel\\Facades\\Messaging"
            }
        }
    }
}
```

自动发现不生效时手动注册：

```php
// Laravel 11+：bootstrap/providers.php
return [
    Erikwang2013\Messaging\Integration\Laravel\MessagingServiceProvider::class,
];

// Laravel 10 及更早：config/app.php
'providers' => [
    Erikwang2013\Messaging\Integration\Laravel\MessagingServiceProvider::class,
],
'aliases' => [
    'Messaging' => Erikwang2013\Messaging\Integration\Laravel\Facades\Messaging::class,
],
```

## 配置

发布配置文件（可选，不发布时 `mergeConfigFrom` 会使用包内默认值）：

```bash
php artisan vendor:publish --tag=messaging-config
```

发布后的 `config/messaging.php`：

```php
return [
    'default' => env('MESSAGING_CONNECTION', 'redis'),

    'connections' => [
        'redis' => [
            'driver' => 'redis',
            'host' => env('MESSAGING_REDIS_HOST', '127.0.0.1'),
            'port' => (int) env('MESSAGING_REDIS_PORT', 6379),
            'password' => env('MESSAGING_REDIS_PASSWORD'),
            'database' => (int) env('MESSAGING_REDIS_DB', 0),
            'group' => env('MESSAGING_REDIS_GROUP', 'messaging'),
            'consumer' => env('MESSAGING_REDIS_CONSUMER'),
        ],

        'beanstalkd' => [
            'driver' => 'beanstalkd',
            'host' => env('MESSAGING_BEANSTALKD_HOST', '127.0.0.1'),
            'port' => (int) env('MESSAGING_BEANSTALKD_PORT', 11300),
        ],
    ],
];
```

结构即 `Manager` 的配置：`default` 指定默认连接名，`connections.<name>.driver` 必填，其余键由对应驱动解释。其他驱动按同样方式追加：

```php
'nats' => [
    'driver' => 'nats',
    'host' => env('MESSAGING_NATS_HOST', '127.0.0.1'),
    'port' => (int) env('MESSAGING_NATS_PORT', 4222),
    'user' => env('MESSAGING_NATS_USER'),
    'pass' => env('MESSAGING_NATS_PASS'),
],
'nsq' => [
    'driver' => 'nsq',
    'host' => env('MESSAGING_NSQ_HOST', '127.0.0.1'),
    'port' => (int) env('MESSAGING_NSQ_PORT', 4150),
    'channel' => env('MESSAGING_NSQ_CHANNEL', 'default'),
],
'activemq' => [
    'driver' => 'activemq',
    'host' => env('MESSAGING_ACTIVEMQ_HOST', '127.0.0.1'),
    'port' => (int) env('MESSAGING_ACTIVEMQ_PORT', 61613),
    'login' => env('MESSAGING_ACTIVEMQ_LOGIN', 'admin'),
    'passcode' => env('MESSAGING_ACTIVEMQ_PASSCODE', 'admin'),
],
'artemis' => [
    'driver' => 'artemis',
    'host' => env('MESSAGING_ARTEMIS_HOST', '127.0.0.1'),
    'port' => (int) env('MESSAGING_ARTEMIS_PORT', 61613),
    'login' => env('MESSAGING_ARTEMIS_LOGIN', 'admin'),
    'passcode' => env('MESSAGING_ARTEMIS_PASSCODE', 'admin'),
],
```

### 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `MESSAGING_CONNECTION` | `redis` | 默认连接名，必须是 `connections` 中已存在的键 |
| `MESSAGING_REDIS_HOST` | `127.0.0.1` | Redis 主机 |
| `MESSAGING_REDIS_PORT` | `6379` | Redis 端口 |
| `MESSAGING_REDIS_PASSWORD` | 空 | Redis 密码 |
| `MESSAGING_REDIS_DB` | `0` | Redis database |
| `MESSAGING_REDIS_GROUP` | `messaging` | Redis Streams 消费者组 |
| `MESSAGING_REDIS_CONSUMER` | 主机名-pid | 消费者名 |
| `MESSAGING_BEANSTALKD_HOST` | `127.0.0.1` | Beanstalkd 主机 |
| `MESSAGING_BEANSTALKD_PORT` | `11300` | Beanstalkd 端口 |

## 使用

`Manager` 是容器单例，可构造器注入，也可用 `app('messaging')` 或 Facade：

```php
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Manager;

final class OrderController
{
    public function __construct(private Manager $messaging)
    {
    }

    public function store(): void
    {
        $this->messaging->connection()->publish('orders', Message::json(['id' => 1]));
        $this->messaging->connection('beanstalkd')->publish('jobs', 'send-mail');
    }
}
```

```php
use Erikwang2013\Messaging\Integration\Laravel\Facades\Messaging;

Messaging::connection('redis')->publish('orders', 'hello');
```

消费放常驻命令里（同步阻塞 API，不要在 HTTP 请求里 `consume()`）：

```php
use Erikwang2013\Messaging\Manager;

final class ConsumeOrders extends \Illuminate\Console\Command
{
    protected $signature = 'orders:consume';

    public function handle(Manager $messaging): int
    {
        $connection = $messaging->connection();

        try {
            $connection->consume('orders', function ($message): void {
                // 处理消息
                $message->ack();
            });
        } finally {
            $messaging->close();
        }

        return self::SUCCESS;
    }
}
```

## 支持的驱动

当前内置六种驱动：`activemq`、`artemis`、`beanstalkd`、`nats`、`nsq`、`redis`。支持矩阵、各驱动配置键与能力差异见 [docs/drivers.md](../drivers.md)；是否支持 ack / requeue / delay / consumer group 以 `$connection->capabilities()` 为准，不支持时驱动会抛 `UnsupportedFeatureException`，不会静默降级。

## 注意事项

- 同步阻塞 API：HTTP 请求只做 publish，消费交给 `php artisan` 常驻命令或队列 worker。
- `Manager` 单例在 Octane 等常驻进程中会跨请求复用连接，进程退出前调用 `Manager::close()`。
