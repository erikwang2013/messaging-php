<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Gcp;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Driver\Gcp\GcpPubSubDriver;
use Erikwang2013\Messaging\Driver\Gcp\GcpPubSubFactory;
use PHPUnit\Framework\TestCase;

final class GcpPubSubFactoryTest extends TestCase
{
    public function test_emulator_config_builds_a_driver_without_credentials(): void
    {
        $driver = (new GcpPubSubFactory())->create($this->config([
            'project_id' => 'test-project',
            'emulator_host' => '127.0.0.1:8085',
        ]));

        self::assertInstanceOf(GcpPubSubDriver::class, $driver);
    }

    public function test_service_account_json_builds_a_driver(): void
    {
        $driver = (new GcpPubSubFactory())->create($this->config([
            'project_id' => 'test-project',
            'credentials_json' => GcpTestCredentials::json(),
        ]));

        self::assertInstanceOf(GcpPubSubDriver::class, $driver);
    }

    public function test_service_account_file_builds_a_driver(): void
    {
        $path = (string) tempnam(sys_get_temp_dir(), 'gcp-creds-');
        file_put_contents($path, GcpTestCredentials::json());

        try {
            $driver = (new GcpPubSubFactory())->create($this->config([
                'project_id' => 'test-project',
                'credentials_file' => $path,
            ]));

            self::assertInstanceOf(GcpPubSubDriver::class, $driver);
        } finally {
            unlink($path);
        }
    }

    public function test_project_id_is_required(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('[project_id]');

        (new GcpPubSubFactory())->create($this->config(['emulator_host' => '127.0.0.1:8085']));
    }

    public function test_credentials_are_required_without_an_emulator(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('[credentials_file]');

        (new GcpPubSubFactory())->create($this->config(['project_id' => 'test-project']));
    }

    public function test_missing_credentials_file_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('[credentials_file]');

        (new GcpPubSubFactory())->create($this->config([
            'project_id' => 'test-project',
            'credentials_file' => '/nonexistent/service-account.json',
        ]));
    }

    public function test_invalid_credentials_json_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('not valid JSON');

        (new GcpPubSubFactory())->create($this->config([
            'project_id' => 'test-project',
            'credentials_json' => '{not json',
        ]));
    }

    public function test_credentials_without_a_private_key_are_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('[private_key]');

        (new GcpPubSubFactory())->create($this->config([
            'project_id' => 'test-project',
            'credentials_json' => GcpTestCredentials::json(['private_key' => '']),
        ]));
    }

    public function test_non_service_account_credentials_are_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('service_account');

        (new GcpPubSubFactory())->create($this->config([
            'project_id' => 'test-project',
            'credentials_json' => GcpTestCredentials::json(['type' => 'authorized_user']),
        ]));
    }

    public function test_timeout_must_be_positive(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('[timeout]');

        (new GcpPubSubFactory())->create($this->config([
            'project_id' => 'test-project',
            'emulator_host' => '127.0.0.1:8085',
            'timeout' => 0,
        ]));
    }

    public function test_emulator_host_must_be_a_non_empty_string(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('[emulator_host]');

        (new GcpPubSubFactory())->create($this->config([
            'project_id' => 'test-project',
            'emulator_host' => '',
        ]));
    }

    public function test_endpoint_must_be_a_non_empty_string(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('[endpoint]');

        (new GcpPubSubFactory())->create($this->config([
            'project_id' => 'test-project',
            'emulator_host' => '127.0.0.1:8085',
            'endpoint' => '',
        ]));
    }

    public function test_subscription_must_be_a_non_empty_string(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('[subscription]');

        (new GcpPubSubFactory())->create($this->config([
            'project_id' => 'test-project',
            'emulator_host' => '127.0.0.1:8085',
            'subscription' => '',
        ]));
    }

    /**
     * @param array<string, mixed> $options
     */
    private function config(array $options): ConnectionConfig
    {
        return ConnectionConfig::fromArray('gcp', ['driver' => 'gcp-pubsub'] + $options);
    }
}
