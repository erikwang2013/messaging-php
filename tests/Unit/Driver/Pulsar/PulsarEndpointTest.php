<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Pulsar;

use Erikwang2013\Messaging\Driver\Pulsar\PulsarEndpoint;
use PHPUnit\Framework\TestCase;

final class PulsarEndpointTest extends TestCase
{
    public function test_plain_url_uses_the_default_pulsar_port(): void
    {
        $endpoint = PulsarEndpoint::fromUrl('pulsar://broker.example');

        self::assertSame('broker.example', $endpoint->host());
        self::assertSame(6650, $endpoint->port());
        self::assertFalse($endpoint->tls());
    }

    public function test_tls_url_uses_the_default_secure_port(): void
    {
        $endpoint = PulsarEndpoint::fromUrl('pulsar+ssl://broker.example');

        self::assertSame(6651, $endpoint->port());
        self::assertTrue($endpoint->tls());
    }

    public function test_explicit_port_is_kept(): void
    {
        self::assertSame(16650, PulsarEndpoint::fromUrl('pulsar://127.0.0.1:16650')->port());
        self::assertSame(16651, PulsarEndpoint::fromUrl('pulsar+ssl://127.0.0.1:16651')->port());
    }

    public function test_trailing_slash_is_allowed(): void
    {
        self::assertSame('broker', PulsarEndpoint::fromUrl('pulsar://broker/')->host());
    }

    public function test_with_tls_keeps_the_host_and_port(): void
    {
        $endpoint = PulsarEndpoint::fromUrl('pulsar://broker:6650')->withTls(true);

        self::assertSame('broker', $endpoint->host());
        self::assertSame(6650, $endpoint->port());
        self::assertTrue($endpoint->tls());
    }

    public function test_equals_compares_host_port_and_tls(): void
    {
        $endpoint = PulsarEndpoint::fromUrl('pulsar://broker:6650');

        self::assertTrue($endpoint->equals(PulsarEndpoint::fromUrl('pulsar://broker:6650')));
        self::assertFalse($endpoint->equals(PulsarEndpoint::fromUrl('pulsar://broker:6651')));
        self::assertFalse($endpoint->equals(PulsarEndpoint::fromUrl('pulsar://other:6650')));
        self::assertFalse($endpoint->equals(PulsarEndpoint::fromUrl('pulsar+ssl://broker:6650')));
    }

    public function test_multiple_hosts_are_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('single broker');

        PulsarEndpoint::fromUrl('pulsar://one:6650,two:6650');
    }

    public function test_non_pulsar_schemes_are_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('scheme');

        PulsarEndpoint::fromUrl('http://broker:6650');
    }

    public function test_paths_are_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('path');

        PulsarEndpoint::fromUrl('pulsar://broker/admin');
    }

    public function test_credentials_query_and_fragment_are_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('credentials');

        PulsarEndpoint::fromUrl('pulsar://user:pass@broker:6650');
    }

    public function test_a_missing_host_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        PulsarEndpoint::fromUrl('pulsar://');
    }

    public function test_an_invalid_port_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        PulsarEndpoint::fromUrl('pulsar://broker:99999');
    }
}
