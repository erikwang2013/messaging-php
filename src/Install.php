<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging;

/**
 * webman plugin install hook. webman's support\Plugin calls install()/uninstall()
 * on package install, update and removal (see the app's composer scripts).
 */
final class Install
{
    public const WEBMAN_PLUGIN = true;

    public static function install(): void
    {
        copy_dir(
            __DIR__ . '/../config/plugin/erikwang2013/messaging-php',
            base_path() . '/config/plugin/erikwang2013/messaging-php',
        );
    }

    public static function uninstall(): void
    {
        remove_dir(base_path() . '/config/plugin/erikwang2013/messaging-php');
    }
}
