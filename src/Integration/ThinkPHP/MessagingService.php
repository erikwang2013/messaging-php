<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Integration\ThinkPHP;

use Erikwang2013\Messaging\Manager;
use Psr\Log\LoggerInterface;
use think\Service;

final class MessagingService extends Service
{
    public function register(): void
    {
        $this->app->bind(Manager::class, function (): Manager {
            return new Manager(
                (array) $this->app->config->get('messaging', []),
                $this->app->bound(LoggerInterface::class) ? $this->app->make(LoggerInterface::class) : null,
            );
        });

        // 容器 make() 会缓存闭包结果，两个绑定返回同一个 Manager
        $this->app->bind('messaging', function (): Manager {
            return $this->app->make(Manager::class);
        });
    }
}
