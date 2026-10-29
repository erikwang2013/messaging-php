# webman 集成

webman 是常驻内存、多进程模型，没有服务容器。集成由三部分组成：

| 文件（应用内路径） | 作用 |
|---|---|
| `config/plugin/erikwang2013/messaging-php/app.php` | 插件开关，`enable` 为 `true` 时 webman 才加载同目录其余配置 |
| `config/plugin/erikwang2013/messaging-php/messaging.php` | 连接配置，即 `Manager` 的配置数组 |
| `config/plugin/erikwang2013/messaging-php/bootstrap.php` | 声明 worker 启动时执行的 Bootstrap 类 |

`Bootstrap::start()` 会在每个 worker（HTTP worker 与自定义进程）启动时调用 `Messaging::boot(config(...))`，因此业务代码里直接取连接即可。

## 安装

```bash
composer config allow-plugins.erikwang2013/messaging-php true
composer require erikwang2013/messaging-php
```

本包是 Composer Plugin。安装/更新时若检测到 `workerman/webman-framework`，会自动把插件配置复制到 `config/plugin/erikwang2013/messaging-php/`（已存在则不覆盖）。

插件通过 `extra.plugin-optional` 声明为可选，但该声明只有 Composer 2.5.3+ 支持：在 2.5.3+ 上未配置 `allow-plugins` 时会跳过插件并继续安装，Composer 2.2–2.5.2 则会安装失败；请保留上面的 `allow-plugins` 命令。插件被跳过时，按下面的手动复制步骤操作即可。

如果项目禁止 Composer 插件，可手动复制：

```bash
mkdir -p config/plugin/erikwang2013/messaging-php
cp vendor/erikwang2013/messaging-php/config/plugin/erikwang2013/messaging-php/*.php \
   config/plugin/erikwang2013/messaging-php/
```

复制后无需修改 `config/bootstrap.php` 或 `config/process.php`，webman 会自动读取插件目录；`app.php` 的 `enable` 置为 `false` 可停用本插件。

## 配置

`config/plugin/erikwang2013/messaging-php/messaging.php`：

```php
return [
    'default' => getenv('MESSAGING_CONNECTION') ?: 'redis',

    'connections' => [
        'redis' => [
            'driver' => 'redis',
            'host' => getenv('MESSAGING_REDIS_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('MESSAGING_REDIS_PORT') ?: 6379),
            'password' => getenv('MESSAGING_REDIS_PASSWORD') ?: null,
            'database' => (int) (getenv('MESSAGING_REDIS_DB') ?: 0),
            'group' => getenv('MESSAGING_REDIS_GROUP') ?: 'messaging',
            'consumer' => getenv('MESSAGING_REDIS_CONSUMER') ?: null,
        ],

        'beanstalkd' => [
            'driver' => 'beanstalkd',
            'host' => getenv('MESSAGING_BEANSTALKD_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('MESSAGING_BEANSTALKD_PORT') ?: 11300),
        ],
    ],
];
```

结构即 `Manager` 的配置：`default` 指定默认连接名，`connections.<name>.driver` 必填，其余键由对应驱动解释。其他驱动按同样方式追加：

```php
'nats' => [
    'driver' => 'nats',
    'host' => getenv('MESSAGING_NATS_HOST') ?: '127.0.0.1',
    'port' => (int) (getenv('MESSAGING_NATS_PORT') ?: 4222),
    'user' => getenv('MESSAGING_NATS_USER') ?: null,
    'pass' => getenv('MESSAGING_NATS_PASS') ?: null,
],
'nsq' => [
    'driver' => 'nsq',
    'host' => getenv('MESSAGING_NSQ_HOST') ?: '127.0.0.1',
    'port' => (int) (getenv('MESSAGING_NSQ_PORT') ?: 4150),
    'channel' => getenv('MESSAGING_NSQ_CHANNEL') ?: 'default',
],
'activemq' => [
    'driver' => 'activemq',
    'host' => getenv('MESSAGING_ACTIVEMQ_HOST') ?: '127.0.0.1',
    'port' => (int) (getenv('MESSAGING_ACTIVEMQ_PORT') ?: 61613),
    'login' => getenv('MESSAGING_ACTIVEMQ_LOGIN') ?: 'admin',
    'passcode' => getenv('MESSAGING_ACTIVEMQ_PASSCODE') ?: 'admin',
],
'artemis' => [
    'driver' => 'artemis',
    'host' => getenv('MESSAGING_ARTEMIS_HOST') ?: '127.0.0.1',
    'port' => (int) (getenv('MESSAGING_ARTEMIS_PORT') ?: 61613),
    'login' => getenv('MESSAGING_ARTEMIS_LOGIN') ?: 'admin',
    'passcode' => getenv('MESSAGING_ARTEMIS_PASSCODE') ?: 'admin',
],
```

webman 启动时会自动加载应用根目录 `.env`（Dockenv），因此配置里用 `getenv()` 读取。变量名与 Laravel / ThinkPHP 示例一致：

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

插件 Bootstrap 已在每个 worker 启动时完成 `Messaging::boot()`，控制器、服务、自定义进程里直接取连接：

```php
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Integration\Webman\Messaging;

// 默认连接（messaging.php 的 default）
Messaging::connection()->publish('orders', Message::json(['id' => 1]));

// 指定连接
Messaging::connection('beanstalkd')->publish('jobs', 'send-mail');

// 需要 Manager 本身
$manager = Messaging::instance();
```

自定义进程消费（阻塞式 `consume()` 只放在专用 consumer 进程里）：

```php
// config/process.php
return [
    'messaging-consumer' => [
        'handler' => process\MessagingConsumer::class,
        'count' => 4,
    ],
];
```

```php
namespace process;

use Erikwang2013\Messaging\Integration\Webman\Messaging;
use Workerman\Worker;

final class MessagingConsumer
{
    public function onWorkerStart(Worker $worker): void
    {
        // 插件 bootstrap 已在当前 worker 执行过 Messaging::boot()
        Messaging::connection()->consume('orders', function ($message): void {
            // 处理消息
            $message->ack();
        });
    }

    public function onWorkerStop(Worker $worker): void
    {
        Messaging::close();
    }
}
```

不使用插件配置时，也可以在进程启动代码里手动 boot：

```php
Messaging::boot(config(Messaging::CONFIG_KEY, []));
```

## 支持的驱动

内置 20 个驱动名（13 套协议栈）。完整清单、各驱动配置键与能力差异见 [docs/drivers.md](../drivers.md)；是否支持 ack / requeue / delay / consumer group 以 `$connection->capabilities()` 为准，不支持时驱动会抛 `UnsupportedFeatureException`，不会静默降级。

## 注意事项

- `Manager` 是**进程级**单例：每个 worker 各自持有连接，进程间不共享；进程退出前调用 `Messaging::close()`。
- 同步阻塞 API：HTTP worker 里只做 publish；`consume()` 会阻塞当前进程，必须放在专用自定义进程中。
