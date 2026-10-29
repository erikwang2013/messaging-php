<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Azure;

/**
 * Shared Access Signature tokens for the Service Bus REST API.
 *
 * Format and signing algorithm:
 * https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-sas#generation-of-a-sas-token
 *
 * The key is used as the literal string from the portal; it is not base64-decoded.
 */
final class SasToken
{
    private const DEFAULT_TTL_SECONDS = 3600;
    private const RENEWAL_WINDOW_SECONDS = 300;

    /** @var array<string, array{token: string, expires: int}> */
    private array $tokens = [];

    public function __construct(
        private string $keyName,
        private string $key,
        private int $ttlSeconds = self::DEFAULT_TTL_SECONDS,
        private int $renewalWindowSeconds = self::RENEWAL_WINDOW_SECONDS,
    ) {
        if ($keyName === '' || $key === '') {
            throw new \InvalidArgumentException('A SAS key name and key are required.');
        }

        if ($ttlSeconds <= 0 || $ttlSeconds <= $renewalWindowSeconds) {
            throw new \InvalidArgumentException('SAS token TTL must be positive and larger than the renewal window.');
        }

        if ($renewalWindowSeconds < 0) {
            throw new \InvalidArgumentException('SAS renewal window cannot be negative.');
        }
    }

    /**
     * Returns a token cached per resource URI until it enters the renewal window.
     */
    public function forResource(string $resourceUri, ?int $now = null): string
    {
        $now ??= time();
        $cached = $this->tokens[$resourceUri] ?? null;

        if ($cached !== null && $cached['expires'] - $this->renewalWindowSeconds > $now) {
            return $cached['token'];
        }

        $expires = $now + $this->ttlSeconds;
        $token = self::generate($resourceUri, $this->keyName, $this->key, $expires);
        $this->tokens[$resourceUri] = ['token' => $token, 'expires' => $expires];

        return $token;
    }

    /**
     * @see https://learn.microsoft.com/en-us/rest/api/servicebus/create-shared-access-signature
     */
    public static function generate(string $resourceUri, string $keyName, string $key, int $expires): string
    {
        if ($resourceUri === '' || $keyName === '' || $key === '') {
            throw new \InvalidArgumentException('A resource URI, SAS key name and key are required.');
        }

        if ($expires <= 0) {
            throw new \InvalidArgumentException('The SAS expiry must be a UNIX timestamp.');
        }

        // The string to sign is the URL-encoded lowercase resource URI, a newline, then the expiry.
        $encodedResource = rawurlencode(strtolower($resourceUri));
        $signature = base64_encode(hash_hmac('sha256', $encodedResource . "\n" . $expires, $key, true));

        return sprintf(
            'SharedAccessSignature sr=%s&sig=%s&se=%d&skn=%s',
            $encodedResource,
            rawurlencode($signature),
            $expires,
            rawurlencode($keyName),
        );
    }
}
