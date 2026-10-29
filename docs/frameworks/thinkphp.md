# ThinkPHP 集成

ThinkPHP 适配层是一个 `think\Service`：从 `config('messaging')` 读取配置，把 `Erikwang2013\Messaging\Manager` 注册到容器，同时提供 `messaging` 绑定。配置结构与原生 `new Manager([...])` 完全一致。

## 安装

```bash
composer config allow-plugins.erikwang2013/messaging-php true
composer require erikwang2013/messaging-php
```

本包是 Composer Plugin。安装/更新时若检测到 `topthink/framework`，会自动把默认配置复制到 `config/messaging.php`（已存在则不覆盖）；ThinkPHP 的 `extra.think` 自动发现仍会注册服务。

插件通过 `extra.plugin-optional` 声明为可选，但该声明只有 Composer 2.5.3+ 支持：在 2.5.3+ 上未配置 `allow-plugins` 时会跳过插件并继续安装，Composer 2.2–2.5.2 则会安装失败；请保留上面的 `allow-plugins` 命令。插件被跳过时，用 `php think vendor:publish` 手动发布配置即可。

## 注册服务

### 自动发现（推荐）

包在 `composer.json` 的 `extra.think` 中声明服务与配置，标准 ThinkPHP 应用的 `post-autoload-dump` 会执行 `php think service:discover` 和 `php think vendor:publish`：

```json
{
    "extra": {
        "think": {
            "services": [
                "Erikwang2013\\Messaging\\Integration\\ThinkPHP\\MessagingService"
            ],
            "config": {
                "messaging": "config/messaging.php"
            }
        }
    }
}
```

- `services` 被 `php think service:discover` 写入 `vendor/services.php`，服务随应用启动注册；
- `config` 被 `php think vendor:publish` 复制到应用 `config/messaging.php`（目标已存在时不覆盖，`-f` 强制覆盖）。

手动触发：

```bash
php think service:discover
php think vendor:publish
```

### 手动注册

不使用自动发现时，先复制配置：

```bash
cp vendor/erikwang2013/messaging-php/config/messaging.php config/messaging.php
```

再把服务加入 `app/service.php`（系统服务定义文件）：

```php
return [
    app\AppService::class,
    Erikwang2013\Messaging\Integration\ThinkPHP\MessagingService::class,
];
```

或在 `app\AppService::register()` 中：

```php
public function register(): void
{
    $this->app->register(\Erikwang2013\Messaging\Integration\ThinkPHP\MessagingService::class);
}
```

## 配置

`config/messaging.php`：

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

ThinkPHP 的 `env()` 读取应用根目录 `.env`，同名变量与 Laravel 示例一致：

```dotenv
MESSAGING_CONNECTION=redis
MESSAGING_REDIS_HOST=127.0.0.1
MESSAGING_REDIS_PORT=6379
MESSAGING_REDIS_PASSWORD=
MESSAGING_REDIS_DB=0
MESSAGING_REDIS_GROUP=messaging
MESSAGING_BEANSTALKD_HOST=127.0.0.1
MESSAGING_BEANSTALKD_PORT=11300
```

## 使用

服务注册后，用 `app('messaging')` 或 `app(Manager::class)` 获取同一实例：

```php
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Manager;

$manager = app('messaging');
$manager->connection()->publish('orders', Message::json(['id' => 1]));

// 或类型注入（容器已绑定 Manager::class）
$manager = app(Manager::class);
```

控制器构造器注入（ThinkPHP 通过容器实例化控制器，已绑定的 `Manager::class` 会注入单例）：

```php
use Erikwang2013\Messaging\Manager;

final class OrderController
{
    public function __construct(private Manager $messaging)
    {
    }

    public function store(): string
    {
        $this->messaging->connection('beanstalkd')->publish('jobs', 'send-mail');

        return 'ok';
    }
}
```

消费放自定义命令（同步阻塞 API，不要在 HTTP 请求里 `consume()`）：

```php
use Erikwang2013\Messaging\Manager;
use think\console\Command;
use think\console\Input;
use think\console\Output;

final class ConsumeOrders extends Command
{
    protected function configure(): void
    {
        $this->setName('orders:consume');
    }

    protected function execute(Input $input, Output $output): int
    {
        $manager = $this->getApp()->make('messaging');
        $connection = $manager->connection();

        try {
            $connection->consume('orders', function ($message): void {
                // 处理消息
                $message->ack();
            });
        } finally {
            $manager->close();
        }

        return 0;
    }
}
```

## 支持的驱动

内置 20 个驱动名（13 套协议栈）。完整清单、各驱动配置键与能力差异见 [docs/drivers.md](../drivers.md)；是否支持 ack / requeue / delay / consumer group 以 `$connection->capabilities()` 为准，不支持时驱动会抛 `UnsupportedFeatureException`，不会静默降级。

## 注意事项

- 同步阻塞 API：HTTP 请求只做 publish，消费交给 `php think` 常驻命令。
- PHP-FPM 下每个请求都会重新构造容器与 `Manager`，短生命周期请求可忽略连接复用；Swoole / workerman 等常驻运行时每个 worker 一个 `Manager`，进程退出前调用 `Manager::close()`。
