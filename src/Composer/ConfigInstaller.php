<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Composer;

/**
 * Copies framework-appropriate default config into a consumer project.
 *
 * Existing files are never overwritten. Yii3 is intentionally absent:
 * yiisoft/config merges the package's config-plugin files at runtime.
 */
final class ConfigInstaller
{
    /**
     * @param list<string> $installedPackages Composer package names in the project
     * @return list<string> paths copied, relative to the project root
     */
    public function install(string $projectRoot, string $packageRoot, array $installedPackages): array
    {
        $copied = [];

        if ($this->hasPackage($installedPackages, 'laravel/framework')) {
            $copied = array_merge($copied, $this->copyFile(
                $packageRoot . '/config/messaging.php',
                $projectRoot . '/config/messaging.php',
                $projectRoot,
            ));
        }

        if ($this->hasPackage($installedPackages, 'topthink/framework')) {
            $copied = array_merge($copied, $this->copyFile(
                $packageRoot . '/config/messaging.php',
                $projectRoot . '/config/messaging.php',
                $projectRoot,
            ));
        }

        if ($this->hasPackage($installedPackages, 'workerman/webman-framework')) {
            $copied = array_merge($copied, $this->copyDirectory(
                $packageRoot . '/config/plugin/erikwang2013/messaging-php',
                $projectRoot . '/config/plugin/erikwang2013/messaging-php',
                $projectRoot,
            ));
        }

        if ($this->hasPackage($installedPackages, 'hyperf/framework')) {
            $copied = array_merge($copied, $this->copyFile(
                $packageRoot . '/src/Integration/Hyperf/config/messaging.php',
                $projectRoot . '/config/autoload/messaging.php',
                $projectRoot,
            ));
        }

        if ($this->hasPackage($installedPackages, 'yiisoft/yii2')) {
            $copied = array_merge($copied, $this->copyFile(
                $packageRoot . '/src/Integration/Yii2/config/messaging.php',
                $projectRoot . '/config/messaging.php',
                $projectRoot,
            ));
        }

        return $copied;
    }

    /**
     * @param list<string> $installedPackages
     */
    private function hasPackage(array $installedPackages, string $package): bool
    {
        return in_array($package, $installedPackages, true);
    }

    /**
     * @return list<string>
     */
    private function copyFile(string $source, string $target, string $projectRoot): array
    {
        if (!is_file($source) || file_exists($target)) {
            return [];
        }

        $directory = dirname($target);
        if (!is_dir($directory) && !@mkdir($directory, 0775, true) && !is_dir($directory)) {
            throw new \RuntimeException(sprintf('Unable to create directory [%s].', $directory));
        }

        if (!@copy($source, $target)) {
            throw new \RuntimeException(sprintf('Unable to copy [%s] to [%s].', $source, $target));
        }

        return [$this->relativePath($target, $projectRoot)];
    }

    /**
     * @return list<string>
     */
    private function copyDirectory(string $source, string $target, string $projectRoot): array
    {
        if (!is_dir($source)) {
            return [];
        }

        $copied = [];
        foreach (glob($source . '/*.php') ?: [] as $file) {
            $copied = array_merge($copied, $this->copyFile(
                $file,
                $target . '/' . basename($file),
                $projectRoot,
            ));
        }

        return $copied;
    }

    private function relativePath(string $path, string $projectRoot): string
    {
        $prefix = rtrim($projectRoot, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR;

        return str_starts_with($path, $prefix) ? substr($path, strlen($prefix)) : $path;
    }
}
