# Yii2 集成

`MessagingComponent` 是一个常规 Yii2 应用组件：`default` 与 `connections` 两个属性直接对应
`Manager` 的配置数组，`connection()` 转发到 `Manager::connection()`，Manager 在第一次使用时
才构建（bootstrap 阶段不建连接）。

## 安装

```bash
composer require erikwang2013/messaging-php
```

Yii2 没有 Composer 自动发现，需要手动注册组件。

## 注册组件

```php
// config/web.php、config/console.php
'components' => [
    'messaging' => [
        'class' => \Erikwang2013\Messaging\Integration\Yii2\MessagingComponent::class,
        'default' => 'primary',
        'connections' => [
            'primary' => [
                'driver' => 'beanstalkd',
                'host' => '127.0.0.1',
                'port' => 11300,
            ],
            'events' => [
                'driver' => 'redis',
                'host' => '127.0.0.1',
                'port' => 6379,
                'group' => 'orders',
            ],
        ],
    ],
],
```

也可以合并包内自带的配置（走环境变量）：

```php
$config = yii\helpers\ArrayHelper::merge(
    $config,
    require __DIR__ . '/../vendor/erikwang2013/messaging-php/src/Integration/Yii2/config/messaging.php',
);
```

`default` 可以省略，缺省时 `Manager` 使用 `connections` 的第一个连接。其余键同
[驱动文档](../drivers.md)。

## 环境变量

自带配置读取下列变量（应用自己注册组件时可直接写死，无需使用）：

| 变量 | 默认 | 说明 |
|---|---|---|
| `MESSAGING_DRIVER` | `beanstalkd` | 驱动名 |
| `MESSAGING_HOST` | `127.0.0.1` | broker 地址 |
| `MESSAGING_PORT` | `11300` | broker 端口；换驱动时一起改 |
| `MESSAGING_TIMEOUT` | `5.0` | 连接超时（秒） |

## 使用

```php
// 默认连接
Yii::$app->messaging->connection()->publish('orders', 'payload');

// 指定连接
Yii::$app->messaging->connection('events')->publish('audit', 'logged');

// 底层 Manager（调用 extend() 注册自定义驱动等）
$manager = Yii::$app->messaging->manager;
```

消费：

```php
use Erikwang2013\Messaging\Contract\ReceiveOptions;

$connection = Yii::$app->messaging->connection();

while ($message = $connection->receive('orders', new ReceiveOptions(5000))) {
    try {
        handle($message->body());
        $message->ack();
    } catch (Throwable $e) {
        $message->reject(requeue: true);
    }
}
```

## 注入 Manager

组件本身没有注册全局单例。需要在构造函数里注入 `Manager` 时，在应用配置加一条容器定义：

```php
'container' => [
    'singletons' => [
        \Erikwang2013\Messaging\Manager::class => static fn () => Yii::$app->messaging->manager,
    ],
],
```

## 注意与限制

- 无框架自动发现：必须注册组件（或合并自带配置文件）。
- 驱动是同步阻塞 API；常驻 console 进程退出前调用 `$manager->close()`。
- Yii 的 `log` 组件不是 PSR-3，未自动接入，组件使用 ``NullLogger``；需要日志时在应用侧
  传入 PSR-3 实现到自定义 Manager，或用 `Yii::$app->messaging->manager` 之外的入口自行构造。
