<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Gcp;

use Erikwang2013\Messaging\Driver\Gcp\ServiceAccountTokenProvider;
use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use GuzzleHttp\Client;
use GuzzleHttp\ClientInterface;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\ResponseInterface;

final class ServiceAccountTokenProviderTest extends TestCase
{
    /** @var list<array{request: \Psr\Http\Message\RequestInterface, response: mixed, options: array<string, mixed>}> */
    private array $history = [];

    public function test_assertion_is_rs256_signed_and_verifiable_with_the_public_key(): void
    {
        $provider = $this->provider([]);

        $jwt = $provider->buildAssertion(1700000000);
        $parts = explode('.', $jwt);
        self::assertCount(3, $parts);

        $header = json_decode(GcpTestCredentials::base64UrlDecode($parts[0]), true, 512, JSON_THROW_ON_ERROR);
        self::assertSame(['alg' => 'RS256', 'typ' => 'JWT', 'kid' => 'test-key-id'], $header);

        $claims = json_decode(GcpTestCredentials::base64UrlDecode($parts[1]), true, 512, JSON_THROW_ON_ERROR);
        self::assertSame('publisher@test-project.iam.gserviceaccount.com', $claims['iss']);
        self::assertSame('https://www.googleapis.com/auth/pubsub', $claims['scope']);
        self::assertSame('https://oauth2.googleapis.com/token', $claims['aud']);
        self::assertSame(1700000000, $claims['iat']);
        self::assertSame(1700003600, $claims['exp']);

        $verified = openssl_verify(
            $parts[0] . '.' . $parts[1],
            GcpTestCredentials::base64UrlDecode($parts[2]),
            GcpTestCredentials::keys()['public'],
            OPENSSL_ALGO_SHA256,
        );
        self::assertSame(1, $verified);
    }

    public function test_tampered_assertion_fails_verification(): void
    {
        $provider = $this->provider([]);
        $parts = explode('.', $provider->buildAssertion(1700000000));

        $signature = GcpTestCredentials::base64UrlDecode($parts[2]);
        $signature[0] = chr(ord($signature[0]) ^ 0xFF);

        self::assertSame(0, openssl_verify(
            $parts[0] . '.' . $parts[1],
            $signature,
            GcpTestCredentials::keys()['public'],
            OPENSSL_ALGO_SHA256,
        ));
    }

    public function test_key_id_is_omitted_when_not_configured(): void
    {
        $provider = $this->provider([], ['private_key_id' => null]);

        $parts = explode('.', $provider->buildAssertion(1700000000));
        $header = json_decode(GcpTestCredentials::base64UrlDecode($parts[0]), true, 512, JSON_THROW_ON_ERROR);

        self::assertArrayNotHasKey('kid', $header);
    }

    public function test_token_exchange_sends_a_signed_jwt_bearer_assertion(): void
    {
        $provider = $this->provider([self::tokenResponse('ya29.test')]);

        self::assertSame('ya29.test', $provider->token());

        self::assertCount(1, $this->history);
        $request = $this->history[0]['request'];
        self::assertSame('POST', $request->getMethod());
        self::assertSame('https://oauth2.googleapis.com/token', (string) $request->getUri());

        $form = [];
        parse_str((string) $request->getBody(), $form);
        self::assertSame('urn:ietf:params:oauth:grant-type:jwt-bearer', $form['grant_type']);

        $parts = explode('.', $form['assertion']);
        self::assertCount(3, $parts);
        self::assertSame(1, openssl_verify(
            $parts[0] . '.' . $parts[1],
            GcpTestCredentials::base64UrlDecode($parts[2]),
            GcpTestCredentials::keys()['public'],
            OPENSSL_ALGO_SHA256,
        ));

        $claims = json_decode(GcpTestCredentials::base64UrlDecode($parts[1]), true, 512, JSON_THROW_ON_ERROR);
        self::assertSame('https://www.googleapis.com/auth/pubsub', $claims['scope']);
        self::assertSame('https://oauth2.googleapis.com/token', $claims['aud']);
    }

    public function test_token_is_cached_until_expiry(): void
    {
        $provider = $this->provider([self::tokenResponse('ya29.first'), self::tokenResponse('ya29.second')]);

        self::assertSame('ya29.first', $provider->token());
        self::assertSame('ya29.first', $provider->token());

        self::assertCount(1, $this->history);
    }

    public function test_token_is_refreshed_inside_the_expiry_skew(): void
    {
        $provider = $this->provider([
            self::tokenResponse('ya29.first', 3600),
            self::tokenResponse('ya29.second', 3600),
        ]);

        self::assertSame('ya29.first', $provider->token());
        self::assertSame('ya29.first', $provider->token());
        self::assertCount(1, $this->history);

        $refreshing = $this->provider([
            self::tokenResponse('ya29.first', 60),
            self::tokenResponse('ya29.second', 60),
        ]);

        self::assertSame('ya29.first', $refreshing->token());
        self::assertSame('ya29.second', $refreshing->token());
        self::assertCount(2, $this->history);
    }

    public function test_missing_access_token_is_an_authentication_error(): void
    {
        $provider = $this->provider([new Response(200, [], '{"expires_in":3600}')]);

        $this->expectException(AuthenticationException::class);

        $provider->token();
    }

    public function test_token_endpoint_error_is_an_authentication_error(): void
    {
        $provider = $this->provider([
            new Response(400, [], '{"error":"invalid_grant","error_description":"Invalid JWT Signature."}'),
        ]);

        try {
            $provider->token();
            self::fail('Expected an AuthenticationException.');
        } catch (AuthenticationException $exception) {
            self::assertStringContainsString('Invalid JWT Signature.', $exception->getMessage());
            self::assertStringContainsString('HTTP 400', $exception->getMessage());
        }
    }

    public function test_invalid_private_key_is_an_authentication_error(): void
    {
        $provider = $this->provider([], ['private_key' => 'not-a-private-key']);

        $this->expectException(AuthenticationException::class);

        $provider->buildAssertion();
    }

    public function test_network_failure_is_a_connection_error(): void
    {
        $provider = $this->provider([
            new ConnectException('Connection refused', new Request('POST', 'https://oauth2.googleapis.com/token'), null, ['errno' => 7]),
        ]);

        $this->expectException(ConnectionException::class);

        $provider->token();
    }

    public function test_timeout_is_a_timeout_error(): void
    {
        $provider = $this->provider([
            new ConnectException('Operation timed out', new Request('POST', 'https://oauth2.googleapis.com/token'), null, ['errno' => 28]),
        ]);

        $this->expectException(TimeoutException::class);

        $provider->token();
    }

    /**
     * @param list<mixed> $responses
     * @param array<string, mixed> $credentialOverrides
     */
    private function provider(array $responses, array $credentialOverrides = []): ServiceAccountTokenProvider
    {
        $container = [];
        $stack = HandlerStack::create(new MockHandler($responses));
        $stack->push(Middleware::history($container));
        $this->history = &$container;

        $client = new Client(['handler' => $stack, 'http_errors' => false]);

        return new ServiceAccountTokenProvider(
            GcpTestCredentials::serviceAccount($credentialOverrides),
            $client,
        );
    }

    private static function tokenResponse(string $token, int $expiresIn = 3600): ResponseInterface
    {
        return new Response(200, ['Content-Type' => 'application/json'], json_encode([
            'access_token' => $token,
            'expires_in' => $expiresIn,
            'token_type' => 'Bearer',
        ], JSON_THROW_ON_ERROR));
    }
}
