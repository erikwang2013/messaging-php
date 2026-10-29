<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Composer;

use Composer\Composer;
use Composer\EventDispatcher\EventSubscriberInterface;
use Composer\IO\IOInterface;
use Composer\Plugin\PluginInterface;
use Composer\Script\Event;
use Composer\Script\ScriptEvents;

/**
 * Composer plugin that copies framework-specific default config on install/update.
 *
 * Composer 2.2+ requires allow-plugins:
 *   composer config allow-plugins.erikwang2013/messaging-php true
 */
final class Plugin implements PluginInterface, EventSubscriberInterface
{
    private ?IOInterface $io = null;

    public function activate(Composer $composer, IOInterface $io): void
    {
        $this->io = $io;
    }

    public function deactivate(Composer $composer, IOInterface $io): void
    {
    }

    public function uninstall(Composer $composer, IOInterface $io): void
    {
    }

    /**
     * @return array<string, string>
     */
    public static function getSubscribedEvents(): array
    {
        return [
            ScriptEvents::POST_INSTALL_CMD => 'installConfigs',
            ScriptEvents::POST_UPDATE_CMD => 'installConfigs',
            ScriptEvents::POST_AUTOLOAD_DUMP => 'installConfigs',
        ];
    }

    public function installConfigs(Event $event): void
    {
        $composer = $event->getComposer();
        $localRepository = $composer->getRepositoryManager()->getLocalRepository();
        $package = $localRepository->findPackage('erikwang2013/messaging-php', '*');
        if ($package === null) {
            return;
        }

        $packageRoot = $composer->getInstallationManager()->getInstallPath($package);
        if ($packageRoot === null) {
            return;
        }

        $projectRoot = dirname((string) $composer->getConfig()->get('vendor-dir'));
        $installedPackages = array_map(
            static fn ($installed): string => $installed->getName(),
            $localRepository->getPackages(),
        );

        try {
            $copied = (new ConfigInstaller())->install($projectRoot, $packageRoot, $installedPackages);
        } catch (\Throwable $exception) {
            $this->io?->writeError('<warning>messaging-php: config copy skipped: ' . $exception->getMessage() . '</warning>');

            return;
        }

        foreach ($copied as $path) {
            $this->io?->write('<info>messaging-php:</info> copied config to ' . $path);
        }
    }
}
