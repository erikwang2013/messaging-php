<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Pulsar;

use Erikwang2013\Messaging\Driver\Pulsar\PulsarErrors;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\MessagingException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use PHPUnit\Framework\TestCase;

final class PulsarErrorsTest extends TestCase
{
    public function test_known_server_errors_have_names(): void
    {
        self::assertSame('AuthenticationError', PulsarErrors::name(3));
        self::assertSame('TopicNotFound', PulsarErrors::name(11));
        self::assertSame('ProducerFenced', PulsarErrors::name(25));
    }

    public function test_unknown_server_errors_keep_their_code(): void
    {
        self::assertSame('UnknownError_99', PulsarErrors::name(99));
    }

    public function test_authentication_and_authorization_errors_map_to_authentication_exception(): void
    {
        self::assertInstanceOf(AuthenticationException::class, PulsarErrors::exception(3, '', 'Connect'));
        self::assertInstanceOf(AuthenticationException::class, PulsarErrors::exception(4, '', 'Connect'));
    }

    public function test_unsupported_version_maps_to_unsupported_feature_exception(): void
    {
        $exception = PulsarErrors::exception(10, 'too old', 'Connect');

        self::assertInstanceOf(UnsupportedFeatureException::class, $exception);
        self::assertStringContainsString('UnsupportedVersionError (10): too old', $exception->getMessage());
    }

    public function test_other_errors_map_to_protocol_exception_with_context(): void
    {
        $exception = PulsarErrors::exception(11, 'topic not found', 'Lookup [events]');

        self::assertInstanceOf(ProtocolException::class, $exception);
        self::assertInstanceOf(MessagingException::class, $exception);
        self::assertSame('Pulsar Lookup [events] failed: TopicNotFound (11): topic not found.', $exception->getMessage());
    }

    public function test_an_empty_server_message_is_omitted(): void
    {
        self::assertSame(
            'Pulsar Connect failed: UnknownError (0).',
            PulsarErrors::exception(0, '', 'Connect')->getMessage(),
        );
    }
}
