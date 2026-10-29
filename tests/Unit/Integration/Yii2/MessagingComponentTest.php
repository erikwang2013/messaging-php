<?php

declare(strict_types=1);

namespace yii\base;

if (!class_exists(Component::class)) {
    /**
     * Stand-in for yii\base\Component so the component's config mapping is testable
     * without installing yiisoft/yii2. Real Yii applies a config array the same way
     * (Yii::configure() assigns every key to the matching property).
     */
    class Component
    {
        /** @param array<string, mixed> $config */
        public function __construct(array $config = [])
        {
            foreach ($config as $name => $value) {
                $this->$name = $value;
            }
        }
    }
}

namespace Erikwang2013\Messaging\Tests\Unit\Integration\Yii2;

use Erikwang2013\Messaging\Config\Config;
use Erikwang2013\Messaging\Integration\Yii2\MessagingComponent;
use Erikwang2013\Messaging\Tests\Support\FakeDriverFactory;
use PHPUnit\Framework\TestCase;

final class MessagingComponentTest extends TestCase
{
    public function test_builds_manager_from_configured_default_and_connections(): void
    {
        $component = new MessagingComponent([
            'default' => 'primary',
            'connections' => [
                'primary' => ['driver' => 'fake'],
            ],
        ]);
        $component->getManager()->extend('fake', new FakeDriverFactory());

        $connection = $component->connection();
        self::assertSame('primary', $connection->name());

        $connection->publish('orders', 'hello');
        self::assertSame('hello', $connection->receive('orders')?->body());
    }

    public function test_falls_back_to_first_connection_and_accepts_explicit_name(): void
    {
        $component = new MessagingComponent([
            'connections' => [
                'first' => ['driver' => 'fake'],
                'second' => ['driver' => 'fake'],
            ],
        ]);
        $component->getManager()->extend('fake', new FakeDriverFactory());

        self::assertSame('first', $component->connection()->name());
        self::assertSame('second', $component->connection('second')->name());
    }

    public function test_shipped_config_is_a_valid_component_registration(): void
    {
        $appConfig = require dirname(__DIR__, 4) . '/src/Integration/Yii2/config/messaging.php';
        $component = $appConfig['components']['messaging'];

        self::assertSame(MessagingComponent::class, $component['class']);

        $config = Config::fromArray([
            'default' => $component['default'],
            'connections' => $component['connections'],
        ]);

        self::assertSame('default', $config->defaultConnection());
        self::assertNotSame('', $config->connection('default')->driver());
    }
}
