<?php

declare(strict_types=1);

// webman 会读取 config/plugin/<vendor>/<name>/bootstrap.php 并在每个 worker 启动时执行
return [
    Erikwang2013\Messaging\Integration\Webman\Bootstrap::class,
];
