<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Gcp;

/**
 * Generates a throwaway RSA key pair so JWT signatures can be verified in tests
 * without shipping a private key fixture.
 */
final class GcpTestCredentials
{
    /** @var array{private: string, public: string}|null */
    private static ?array $keys = null;

    /**
     * @return array{private: string, public: string}
     */
    public static function keys(): array
    {
        if (self::$keys !== null) {
            return self::$keys;
        }

        $resource = openssl_pkey_new([
            'private_key_bits' => 2048,
            'private_key_type' => OPENSSL_KEYTYPE_RSA,
        ]);
        if ($resource === false) {
            throw new \RuntimeException('Unable to generate a test RSA key pair.');
        }

        $private = '';
        if (!openssl_pkey_export($resource, $private)) {
            throw new \RuntimeException('Unable to export the test RSA private key.');
        }

        $details = openssl_pkey_get_details($resource);
        if ($details === false) {
            throw new \RuntimeException('Unable to read the test RSA public key.');
        }

        return self::$keys = ['private' => $private, 'public' => $details['key']];
    }

    /**
     * @param array<string, mixed> $overrides
     * @return array<string, mixed>
     */
    public static function serviceAccount(array $overrides = []): array
    {
        return $overrides + [
            'type' => 'service_account',
            'project_id' => 'test-project',
            'private_key_id' => 'test-key-id',
            'private_key' => self::keys()['private'],
            'client_email' => 'publisher@test-project.iam.gserviceaccount.com',
            'token_uri' => 'https://oauth2.googleapis.com/token',
        ];
    }

    /**
     * @param array<string, mixed> $overrides
     */
    public static function json(array $overrides = []): string
    {
        return json_encode(self::serviceAccount($overrides), JSON_THROW_ON_ERROR);
    }

    public static function base64UrlDecode(string $value): string
    {
        $decoded = base64_decode(strtr($value, '-_', '+/') . str_repeat('=', (4 - strlen($value) % 4) % 4), true);
        if ($decoded === false) {
            throw new \RuntimeException('Invalid base64url value in test.');
        }

        return $decoded;
    }
}
