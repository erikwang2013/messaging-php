<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Gcp;

use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use GuzzleHttp\ClientInterface;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\GuzzleException;
use Psr\Log\LoggerInterface;

/**
 * Exchanges a service-account JWT assertion for a Google OAuth 2.0 access token.
 *
 * Pure PHP: the RS256 signature is produced with ext-openssl.
 */
final class ServiceAccountTokenProvider
{
    private const SCOPE = 'https://www.googleapis.com/auth/pubsub';
    private const TOKEN_URI = 'https://oauth2.googleapis.com/token';
    private const JWT_LIFETIME_SECONDS = 3600;
    private const EXPIRY_SKEW_SECONDS = 60;

    /** @var array<string, mixed> */
    private array $credentials;
    private ClientInterface $http;
    private float $timeout;
    private ?LoggerInterface $logger;

    private ?string $accessToken = null;
    private int $expiresAt = 0;

    /**
     * @param array<string, mixed> $credentials Parsed service-account JSON.
     */
    public function __construct(
        array $credentials,
        ClientInterface $http,
        float $timeout = 30.0,
        ?LoggerInterface $logger = null,
    ) {
        $this->credentials = $credentials;
        $this->http = $http;
        $this->timeout = $timeout;
        $this->logger = $logger;
    }

    public function token(): string
    {
        if ($this->accessToken !== null && time() < $this->expiresAt) {
            return $this->accessToken;
        }

        $response = $this->requestToken($this->buildAssertion());
        $status = $response->getStatusCode();
        $raw = (string) $response->getBody();

        if ($status >= 400) {
            throw new AuthenticationException(sprintf(
                'Google OAuth token request failed with HTTP %d: %s',
                $status,
                self::errorDetail($raw),
            ));
        }

        $data = json_decode($raw, true);
        $token = is_array($data) ? ($data['access_token'] ?? null) : null;
        if (!is_string($token) || $token === '') {
            throw new AuthenticationException('Google OAuth token response did not contain an access token.');
        }

        $expiresIn = is_array($data) && is_numeric($data['expires_in'] ?? null) ? (int) $data['expires_in'] : 3600;
        $this->accessToken = $token;
        $this->expiresAt = time() + max(0, $expiresIn - self::EXPIRY_SKEW_SECONDS);
        $this->logger?->debug('Obtained Google Cloud access token.', ['expires_at' => $this->expiresAt]);

        return $this->accessToken;
    }

    /**
     * @param int|null $issuedAt Unix timestamp, defaults to now; exposed for testing.
     */
    public function buildAssertion(?int $issuedAt = null): string
    {
        $issuedAt ??= time();

        $header = ['alg' => 'RS256', 'typ' => 'JWT'];
        $keyId = $this->credentials['private_key_id'] ?? null;
        if (is_string($keyId) && $keyId !== '') {
            $header['kid'] = $keyId;
        }

        $claims = [
            'iss' => (string) ($this->credentials['client_email'] ?? ''),
            'scope' => self::SCOPE,
            'aud' => self::TOKEN_URI,
            'iat' => $issuedAt,
            'exp' => $issuedAt + self::JWT_LIFETIME_SECONDS,
        ];

        $signingInput = self::base64Url(self::encodeJson($header)) . '.' . self::base64Url(self::encodeJson($claims));

        $privateKey = $this->credentials['private_key'] ?? null;
        $key = is_string($privateKey) ? openssl_pkey_get_private($privateKey) : false;
        if ($key === false) {
            throw new AuthenticationException('The service-account private key could not be loaded.');
        }

        $signature = '';
        if (!openssl_sign($signingInput, $signature, $key, OPENSSL_ALGO_SHA256)) {
            throw new AuthenticationException('The service-account JWT could not be signed.');
        }

        return $signingInput . '.' . self::base64Url($signature);
    }

    private function requestToken(string $assertion): \Psr\Http\Message\ResponseInterface
    {
        try {
            return $this->http->request('POST', self::TOKEN_URI, [
                'form_params' => [
                    'grant_type' => 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                    'assertion' => $assertion,
                ],
                'headers' => ['Accept' => 'application/json'],
                'timeout' => $this->timeout,
                'http_errors' => false,
            ]);
        } catch (ConnectException $exception) {
            if (($exception->getHandlerContext()['errno'] ?? null) === 28) {
                throw new TimeoutException('Google OAuth token request timed out.', 0, $exception);
            }

            throw new ConnectionException('Google OAuth token request failed: ' . $exception->getMessage(), 0, $exception);
        } catch (GuzzleException $exception) {
            throw new ConnectionException('Google OAuth token request failed: ' . $exception->getMessage(), 0, $exception);
        }
    }

    /**
     * @param array<string, mixed> $value
     */
    private static function encodeJson(array $value): string
    {
        try {
            return json_encode($value, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
        } catch (\JsonException $exception) {
            throw new AuthenticationException('Unable to encode the service-account JWT.', 0, $exception);
        }
    }

    private static function base64Url(string $value): string
    {
        return rtrim(strtr(base64_encode($value), '+/', '-_'), '=');
    }

    private static function errorDetail(string $raw): string
    {
        $data = json_decode($raw, true);
        if (!is_array($data)) {
            return $raw === '' ? 'empty response' : $raw;
        }

        $description = $data['error_description'] ?? null;
        if (is_string($description) && $description !== '') {
            return $description;
        }

        $error = $data['error'] ?? null;
        if (is_string($error) && $error !== '') {
            return $error;
        }

        if (is_array($error) && is_string($error['message'] ?? null)) {
            return $error['message'];
        }

        return 'unexpected token response';
    }
}
