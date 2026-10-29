<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Azure;

use Erikwang2013\Messaging\Driver\Azure\SasToken;
use PHPUnit\Framework\TestCase;

final class SasTokenTest extends TestCase
{
    private const KEY_NAME = 'RootManageSharedAccessKey';
    private const KEY = 'dGVzdC1rZXktZm9yLXNlcnZpY2UtYnVz';

    /**
     * Vector computed with an independent HMAC-SHA256 implementation from the
     * algorithm documented at
     * https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-sas#generation-of-a-sas-token
     */
    public function test_generate_matches_the_documented_algorithm(): void
    {
        $token = SasToken::generate(
            'https://example.servicebus.windows.net/orders',
            self::KEY_NAME,
            self::KEY,
            1700000000,
        );

        self::assertSame(
            'SharedAccessSignature sr=https%3A%2F%2Fexample.servicebus.windows.net%2Forders'
            . '&sig=5f3Kb%2FOMKHeq4U%2FMFKS18IFughjgA9a8m1uGh5SslZA%3D'
            . '&se=1700000000&skn=RootManageSharedAccessKey',
            $token,
        );
    }

    public function test_generate_canonicalises_the_resource_uri_to_lowercase(): void
    {
        $token = SasToken::generate(
            'https://Example.ServiceBus.Windows.Net/Orders',
            self::KEY_NAME,
            self::KEY,
            1700000000,
        );

        self::assertSame(
            'SharedAccessSignature sr=https%3A%2F%2Fexample.servicebus.windows.net%2Forders'
            . '&sig=5f3Kb%2FOMKHeq4U%2FMFKS18IFughjgA9a8m1uGh5SslZA%3D'
            . '&se=1700000000&skn=RootManageSharedAccessKey',
            $token,
        );
    }

    public function test_generate_encodes_a_namespace_resource(): void
    {
        $token = SasToken::generate(
            'https://example.servicebus.windows.net/',
            self::KEY_NAME,
            self::KEY,
            1700000000,
        );

        self::assertSame(
            'SharedAccessSignature sr=https%3A%2F%2Fexample.servicebus.windows.net%2F'
            . '&sig=SyG3kjSY6zyh3fbL3hjsc%2Fry6pyjFs183T8niSulc9c%3D'
            . '&se=1700000000&skn=RootManageSharedAccessKey',
            $token,
        );
    }

    public function test_tokens_are_cached_per_resource_until_the_renewal_window(): void
    {
        $provider = new SasToken(self::KEY_NAME, self::KEY);
        $resource = 'https://example.servicebus.windows.net/orders';

        $first = $provider->forResource($resource, 1000);
        self::assertSame($first, $provider->forResource($resource, 1001));
        self::assertStringContainsString('&se=4600&', $first);

        $refreshed = $provider->forResource($resource, 4300);
        self::assertNotSame($first, $refreshed);
        self::assertStringContainsString('&se=7900&', $refreshed);
    }

    public function test_each_resource_gets_its_own_token(): void
    {
        $provider = new SasToken(self::KEY_NAME, self::KEY);

        $orders = $provider->forResource('https://example.servicebus.windows.net/orders', 1000);
        $billing = $provider->forResource('https://example.servicebus.windows.net/billing', 1000);

        self::assertNotSame($orders, $billing);
        self::assertStringContainsString('%2Forders&', $orders);
        self::assertStringContainsString('%2Fbilling&', $billing);
    }

    public function test_invalid_constructor_arguments_are_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        new SasToken('', self::KEY);
    }

    public function test_invalid_expiry_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        SasToken::generate('https://example.servicebus.windows.net/', self::KEY_NAME, self::KEY, 0);
    }
}
