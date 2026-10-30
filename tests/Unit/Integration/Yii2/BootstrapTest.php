<?php

declare(strict_types=1);

namespace yii\base;

if (!interface_exists(BootstrapInterface::class)) {
    /**
     * Stand-in for yii\base\BootstrapInterface so the extension bootstrap is testable
     * without installing yiisoft/yii2. Real Yii instantiates the class listed in
     * extra.bootstrap and calls bootstrap($app) when the application boots.
     */
    interface BootstrapInterface
    {
        public function bootstrap($app);
    }
}

namespace Erikwang2013\Messaging\Tests\Unit\Integration\Yii2;

use Erikwang2013\Messaging\Integration\Yii2\Bootstrap;
use Erikwang2013\Messaging\Integration\Yii2\MessagingComponent;
use PHPUnit\Framework\TestCase;

final class BootstrapTest extends TestCase
{
    public function test_registers_messaging_component_from_shipped_config(): void
    {
        $app = new FakeApplication();

        (new Bootstrap())->bootstrap($app);

        self::assertArrayHasKey('messaging', $app->components);
        self::assertSame(MessagingComponent::class, $app->components['messaging']['class']);
        self::assertSame('default', $app->components['messaging']['default']);
        self::assertArrayHasKey('default', $app->components['messaging']['connections']);
    }

    public function test_existing_component_is_not_overwritten(): void
    {
        $app = new FakeApplication(['messaging' => ['class' => MessagingComponent::class]]);

        (new Bootstrap())->bootstrap($app);

        self::assertSame(['class' => MessagingComponent::class], $app->components['messaging']);
    }
}

final class FakeApplication
{
    /**
     * @param array<string, mixed> $components
     */
    public function __construct(public array $components = [])
    {
    }

    public function has(string $id): bool
    {
        return isset($this->components[$id]);
    }

    /**
     * @param mixed $definition
     */
    public function set(string $id, $definition): void
    {
        $this->components[$id] = $definition;
    }
}
