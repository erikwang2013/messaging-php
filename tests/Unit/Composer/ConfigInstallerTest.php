<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Composer;

use Erikwang2013\Messaging\Composer\ConfigInstaller;
use PHPUnit\Framework\TestCase;

final class ConfigInstallerTest extends TestCase
{
    private string $root;
    private string $package;

    protected function setUp(): void
    {
        $this->root = sys_get_temp_dir() . '/messaging-config-' . bin2hex(random_bytes(4));
        $this->package = $this->root . '/package';

        mkdir($this->package . '/config/plugin/erikwang2013/messaging-php', 0775, true);
        mkdir($this->package . '/src/Integration/Hyperf/config', 0775, true);
        mkdir($this->package . '/src/Integration/Yii2/config', 0775, true);

        file_put_contents($this->package . '/config/messaging.php', "<?php return ['laravel' => true];\n");
        file_put_contents($this->package . '/config/plugin/erikwang2013/messaging-php/app.php', "<?php return [];\n");
        file_put_contents($this->package . '/config/plugin/erikwang2013/messaging-php/messaging.php', "<?php return [];\n");
        file_put_contents($this->package . '/config/plugin/erikwang2013/messaging-php/bootstrap.php', "<?php return [];\n");
        file_put_contents($this->package . '/src/Integration/Hyperf/config/messaging.php', "<?php return ['hyperf' => true];\n");
        file_put_contents($this->package . '/src/Integration/Yii2/config/messaging.php', "<?php return ['yii2' => true];\n");
    }

    protected function tearDown(): void
    {
        $this->removeDirectory($this->root);
    }

    public function test_laravel_config_is_copied(): void
    {
        $copied = $this->installer()->install($this->root, $this->package, ['laravel/framework']);

        self::assertSame(['config/messaging.php'], $copied);
        self::assertFileExists($this->root . '/config/messaging.php');
        self::assertStringContainsString("'laravel' => true", (string) file_get_contents($this->root . '/config/messaging.php'));
    }

    public function test_thinkphp_config_is_copied(): void
    {
        $copied = $this->installer()->install($this->root, $this->package, ['topthink/framework']);

        self::assertSame(['config/messaging.php'], $copied);
        self::assertFileExists($this->root . '/config/messaging.php');
    }

    public function test_webman_plugin_files_are_copied(): void
    {
        $copied = $this->installer()->install($this->root, $this->package, ['workerman/webman-framework']);

        sort($copied);
        self::assertSame([
            'config/plugin/erikwang2013/messaging-php/app.php',
            'config/plugin/erikwang2013/messaging-php/bootstrap.php',
            'config/plugin/erikwang2013/messaging-php/messaging.php',
        ], $copied);
    }

    public function test_hyperf_config_is_copied(): void
    {
        $copied = $this->installer()->install($this->root, $this->package, ['hyperf/framework']);

        self::assertSame(['config/autoload/messaging.php'], $copied);
        self::assertFileExists($this->root . '/config/autoload/messaging.php');
    }

    public function test_yii2_config_is_copied(): void
    {
        $copied = $this->installer()->install($this->root, $this->package, ['yiisoft/yii2']);

        self::assertSame(['config/messaging.php'], $copied);
        self::assertStringContainsString("'yii2' => true", (string) file_get_contents($this->root . '/config/messaging.php'));
    }

    public function test_yii3_needs_no_copy(): void
    {
        self::assertSame([], $this->installer()->install($this->root, $this->package, ['yiisoft/yii']));
    }

    public function test_existing_config_is_not_overwritten(): void
    {
        mkdir($this->root . '/config', 0775, true);
        file_put_contents($this->root . '/config/messaging.php', "<?php return ['user' => true];\n");

        $copied = $this->installer()->install($this->root, $this->package, ['laravel/framework']);

        self::assertSame([], $copied);
        self::assertStringContainsString("'user' => true", (string) file_get_contents($this->root . '/config/messaging.php'));
    }

    private function installer(): ConfigInstaller
    {
        return new ConfigInstaller();
    }

    private function removeDirectory(string $path): void
    {
        if (!is_dir($path)) {
            return;
        }

        $entries = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($path, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST,
        );

        foreach ($entries as $entry) {
            if ($entry->isDir()) {
                @rmdir($entry->getPathname());
            } else {
                @unlink($entry->getPathname());
            }
        }

        @rmdir($path);
    }
}
