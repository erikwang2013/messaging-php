<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Aws;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Driver\Aws\AwsConfig;
use Erikwang2013\Messaging\Driver\Aws\KinesisDriver;
use Erikwang2013\Messaging\Driver\Aws\KinesisFactory;
use Erikwang2013\Messaging\Driver\Aws\SnsDriver;
use Erikwang2013\Messaging\Driver\Aws\SnsFactory;
use Erikwang2013\Messaging\Driver\Aws\SqsDriver;
use Erikwang2013\Messaging\Driver\Aws\SqsFactory;
use PHPUnit\Framework\TestCase;

final class AwsFactoryTest extends TestCase
{
    public function test_factories_instantiate_the_matching_driver(): void
    {
        self::assertInstanceOf(SqsDriver::class, (new SqsFactory())->create($this->config('sqs')));
        self::assertInstanceOf(SnsDriver::class, (new SnsFactory())->create($this->config('sns')));
        self::assertInstanceOf(KinesisDriver::class, (new KinesisFactory())->create($this->config('kinesis')));
    }

    public function test_region_is_required(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('region');

        AwsConfig::fromConnectionConfig(ConnectionConfig::fromArray('aws', [
            'driver' => 'sqs',
            'access_key' => 'AKID',
            'secret_key' => 'SECRET',
        ]));
    }

    public function test_credentials_fall_back_to_environment(): void
    {
        $previousKey = getenv('AWS_ACCESS_KEY_ID');
        $previousSecret = getenv('AWS_SECRET_ACCESS_KEY');
        putenv('AWS_ACCESS_KEY_ID=ENVKEY');
        putenv('AWS_SECRET_ACCESS_KEY=ENVSECRET');

        try {
            $config = AwsConfig::fromConnectionConfig(ConnectionConfig::fromArray('aws', [
                'driver' => 'sqs',
                'region' => 'us-east-1',
            ]));

            self::assertSame('ENVKEY', $config->accessKey);
            self::assertSame('ENVSECRET', $config->secretKey);
        } finally {
            self::restoreEnvironment('AWS_ACCESS_KEY_ID', $previousKey);
            self::restoreEnvironment('AWS_SECRET_ACCESS_KEY', $previousSecret);
        }
    }

    public function test_config_credentials_take_precedence_over_environment(): void
    {
        $previousKey = getenv('AWS_ACCESS_KEY_ID');
        putenv('AWS_ACCESS_KEY_ID=ENVKEY');

        try {
            $config = AwsConfig::fromConnectionConfig(ConnectionConfig::fromArray('aws', [
                'driver' => 'sqs',
                'region' => 'us-east-1',
                'access_key' => 'CONFIGKEY',
                'secret_key' => 'CONFIGSECRET',
                'session_token' => 'TOKEN',
            ]));

            self::assertSame('CONFIGKEY', $config->accessKey);
            self::assertSame('TOKEN', $config->sessionToken);
        } finally {
            self::restoreEnvironment('AWS_ACCESS_KEY_ID', $previousKey);
        }
    }

    public function test_credentials_are_required(): void
    {
        $previousKey = getenv('AWS_ACCESS_KEY_ID');
        $previousSecret = getenv('AWS_SECRET_ACCESS_KEY');
        putenv('AWS_ACCESS_KEY_ID');
        putenv('AWS_SECRET_ACCESS_KEY');

        try {
            $this->expectException(\InvalidArgumentException::class);

            AwsConfig::fromConnectionConfig(ConnectionConfig::fromArray('aws', [
                'driver' => 'sqs',
                'region' => 'us-east-1',
            ]));
        } finally {
            self::restoreEnvironment('AWS_ACCESS_KEY_ID', $previousKey);
            self::restoreEnvironment('AWS_SECRET_ACCESS_KEY', $previousSecret);
        }
    }

    public function test_endpoint_must_be_http(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        AwsConfig::fromConnectionConfig(ConnectionConfig::fromArray('aws', [
            'driver' => 'sqs',
            'region' => 'us-east-1',
            'access_key' => 'AKID',
            'secret_key' => 'SECRET',
            'endpoint' => 'ftp://127.0.0.1:4566',
        ]));
    }

    public function test_timeout_must_be_positive(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        AwsConfig::fromConnectionConfig(ConnectionConfig::fromArray('aws', [
            'driver' => 'sqs',
            'region' => 'us-east-1',
            'access_key' => 'AKID',
            'secret_key' => 'SECRET',
            'timeout' => 0,
        ]));
    }

    /**
     * @return ConnectionConfig
     */
    private function config(string $driver): ConnectionConfig
    {
        return ConnectionConfig::fromArray($driver, [
            'driver' => $driver,
            'region' => 'us-east-1',
            'access_key' => 'AKID',
            'secret_key' => 'SECRET',
            'timeout' => 2.0,
        ]);
    }

    private static function restoreEnvironment(string $name, string|false $value): void
    {
        if ($value === false) {
            putenv($name);
        } else {
            putenv($name . '=' . $value);
        }
    }
}
