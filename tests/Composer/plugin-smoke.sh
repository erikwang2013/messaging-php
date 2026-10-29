#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

mkdir -p "$TMP/fake-laravel"
cat > "$TMP/fake-laravel/composer.json" <<'JSON'
{
    "name": "laravel/framework",
    "version": "11.0.0",
    "type": "library"
}
JSON

cat > "$TMP/composer.json" <<JSON
{
    "name": "test/messaging-plugin-smoke",
    "minimum-stability": "dev",
    "prefer-stable": true,
    "repositories": [
        {
            "type": "path",
            "url": "$ROOT",
            "options": {"symlink": false}
        },
        {
            "type": "path",
            "url": "$TMP/fake-laravel",
            "options": {"symlink": false}
        }
    ],
    "require": {
        "erikwang2013/messaging-php": "dev-main",
        "laravel/framework": "^11.0"
    },
    "config": {
        "allow-plugins": {
            "erikwang2013/messaging-php": true
        }
    }
}
JSON

cd "$TMP"
COMPOSER_CACHE_DIR="$TMP/cache" composer update --no-interaction --prefer-dist --no-progress

test -f config/messaging.php
grep -q "connections" config/messaging.php

echo "Composer plugin smoke test OK"
