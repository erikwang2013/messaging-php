<?php

declare(strict_types=1);

namespace {
    if (!function_exists('base_path')) {
        /**
         * Stand-in for webman's helper so the install hook is testable without webman.
         */
        function base_path(string $path = ''): string
        {
            $base = (string) getenv('WEBMAN_TEST_BASE_PATH');

            return $path === '' ? $base : $base . '/' . ltrim($path, '/');
        }
    }

    if (!function_exists('copy_dir')) {
        /**
         * Mirror of webman's copy_dir(): existing files are kept unless $overwrite is true.
         */
        function copy_dir(string $source, string $dest, bool $overwrite = false): void
        {
            if (is_dir($source)) {
                if (!is_dir($dest)) {
                    mkdir($dest, 0775, true);
                }
                foreach (scandir($source) ?: [] as $file) {
                    if ($file !== '.' && $file !== '..') {
                        copy_dir($source . '/' . $file, $dest . '/' . $file, $overwrite);
                    }
                }
            } elseif (is_file($source) && ($overwrite || !file_exists($dest))) {
                copy($source, $dest);
            }
        }
    }

    if (!function_exists('remove_dir')) {
        function remove_dir(string $dir): bool
        {
            if (is_link($dir) || is_file($dir)) {
                return unlink($dir);
            }
            foreach (array_diff(scandir($dir) ?: [], ['.', '..']) as $file) {
                is_dir($dir . '/' . $file) ? remove_dir($dir . '/' . $file) : unlink($dir . '/' . $file);
            }

            return rmdir($dir);
        }
    }
}

namespace Erikwang2013\Messaging\Tests\Unit\Integration\Webman {
    use Erikwang2013\Messaging\Install;
    use PHPUnit\Framework\TestCase;

    final class InstallTest extends TestCase
    {
        private string $root;

        protected function setUp(): void
        {
            $this->root = sys_get_temp_dir() . '/messaging-webman-' . bin2hex(random_bytes(4));
            mkdir($this->root, 0775, true);
            putenv('WEBMAN_TEST_BASE_PATH=' . $this->root);
        }

        protected function tearDown(): void
        {
            putenv('WEBMAN_TEST_BASE_PATH');
            remove_dir($this->root);
        }

        public function test_install_copies_plugin_config_without_overwriting(): void
        {
            $target = $this->root . '/config/plugin/erikwang2013/messaging-php';
            mkdir($target, 0775, true);
            file_put_contents($target . '/messaging.php', 'custom');

            Install::install();

            self::assertFileExists($target . '/app.php');
            self::assertFileExists($target . '/bootstrap.php');
            self::assertSame('custom', file_get_contents($target . '/messaging.php'));
        }

        public function test_uninstall_removes_plugin_config(): void
        {
            Install::install();
            $target = $this->root . '/config/plugin/erikwang2013/messaging-php';
            self::assertDirectoryExists($target);

            Install::uninstall();

            self::assertDirectoryDoesNotExist($target);
        }
    }
}
