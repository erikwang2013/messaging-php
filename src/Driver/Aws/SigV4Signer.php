<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Aws;

/**
 * AWS Signature Version 4 signer.
 *
 * Pure implementation of the canonical request, string-to-sign and signing
 * key derivation described in the AWS General Reference.
 */
final class SigV4Signer
{
    private string $accessKey;
    private string $secretKey;
    private ?string $sessionToken;

    public function __construct(string $accessKey, string $secretKey, ?string $sessionToken = null)
    {
        if ($accessKey === '' || $secretKey === '') {
            throw new \InvalidArgumentException('AWS access key and secret key must not be empty.');
        }

        $this->accessKey = $accessKey;
        $this->secretKey = $secretKey;
        $this->sessionToken = $sessionToken === '' ? null : $sessionToken;
    }

    /**
     * Signs a request and returns the headers that must be sent.
     *
     * @param array<string, string> $headers
     * @return array<string, string>
     */
    public function sign(
        string $method,
        string $url,
        array $headers,
        string $payload,
        string $region,
        string $service,
        ?int $timestamp = null,
    ): array {
        $parts = parse_url($url);
        if ($parts === false || !isset($parts['host']) || $parts['host'] === '') {
            throw new \InvalidArgumentException(sprintf('Invalid AWS request URL [%s].', $url));
        }

        $host = $parts['host'];
        if (isset($parts['port'])) {
            $host .= ':' . $parts['port'];
        }

        $timestamp ??= time();
        $amzDate = gmdate('Ymd\THis\Z', $timestamp);
        $dateStamp = gmdate('Ymd', $timestamp);
        $scope = $dateStamp . '/' . $region . '/' . $service . '/aws4_request';

        $signed = self::normalizeHeaders($headers);
        unset($signed['authorization'], $signed['host']);
        $signed['host'] = $host;
        $signed['x-amz-date'] = $amzDate;
        if ($this->sessionToken !== null) {
            $signed['x-amz-security-token'] = $this->sessionToken;
        }

        ksort($signed, SORT_STRING);
        $signedHeaders = implode(';', array_keys($signed));

        $canonicalRequest = self::canonicalRequest(
            $method,
            $parts['path'] ?? '/',
            self::parseQuery($parts['query'] ?? ''),
            $signed,
            hash('sha256', $payload),
        );
        $stringToSign = self::stringToSign($canonicalRequest, $amzDate, $scope);
        $signature = hash_hmac('sha256', $stringToSign, $this->signingKey($dateStamp, $region, $service));

        $signed['authorization'] = sprintf(
            'AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s',
            $this->accessKey,
            $scope,
            $signedHeaders,
            $signature,
        );
        ksort($signed, SORT_STRING);

        return $signed;
    }

    /**
     * @param array<string, mixed> $query Values may be a string or a list of strings.
     * @param array<string, mixed> $headers
     */
    public static function canonicalRequest(string $method, string $uri, array $query, array $headers, string $payloadHash): string
    {
        $normalized = self::normalizeHeaders($headers);
        ksort($normalized, SORT_STRING);

        $canonicalHeaders = '';
        foreach ($normalized as $name => $value) {
            $canonicalHeaders .= $name . ':' . $value . "\n";
        }

        return implode("\n", [
            strtoupper($method),
            self::canonicalUri($uri),
            self::canonicalQuery($query),
            $canonicalHeaders,
            implode(';', array_keys($normalized)),
            strtolower($payloadHash),
        ]);
    }

    /**
     * @param array<string, mixed> $query
     */
    public static function canonicalQuery(array $query): string
    {
        $pairs = [];
        foreach ($query as $name => $values) {
            foreach (is_array($values) ? $values : [$values] as $value) {
                $pairs[] = rawurlencode((string) $name) . '=' . rawurlencode(is_scalar($value) ? (string) $value : '');
            }
        }

        sort($pairs, SORT_STRING);

        return implode('&', $pairs);
    }

    public static function stringToSign(string $canonicalRequest, string $amzDate, string $scope): string
    {
        return "AWS4-HMAC-SHA256\n" . $amzDate . "\n" . $scope . "\n" . hash('sha256', $canonicalRequest);
    }

    public function signingKey(string $dateStamp, string $region, string $service): string
    {
        $dateKey = hash_hmac('sha256', $dateStamp, 'AWS4' . $this->secretKey, true);
        $regionKey = hash_hmac('sha256', $region, $dateKey, true);
        $serviceKey = hash_hmac('sha256', $service, $regionKey, true);

        return hash_hmac('sha256', 'aws4_request', $serviceKey, true);
    }

    /**
     * RFC 3986 encoding: everything except A-Z a-z 0-9 - . _ ~ is percent-encoded.
     */
    public static function uriEncode(string $value, bool $encodeSlash = true): string
    {
        $encoded = rawurlencode($value);

        return $encodeSlash ? $encoded : str_replace('%2F', '/', $encoded);
    }

    /**
     * @param array<string, mixed> $headers
     * @return array<string, string>
     */
    private static function normalizeHeaders(array $headers): array
    {
        $normalized = [];
        foreach ($headers as $name => $value) {
            $name = strtolower(trim((string) $name));
            if ($name === '') {
                continue;
            }

            $normalized[$name] = trim((string) preg_replace('/\s+/', ' ', (string) $value));
        }

        return $normalized;
    }

    private static function canonicalUri(string $uri): string
    {
        $segments = explode('/', $uri === '' ? '/' : $uri);
        foreach ($segments as $index => $segment) {
            $segments[$index] = rawurlencode(rawurldecode($segment));
        }

        $encoded = implode('/', $segments);

        return str_starts_with($encoded, '/') ? $encoded : '/' . $encoded;
    }

    /**
     * @return array<string, list<string>>
     */
    private static function parseQuery(string $query): array
    {
        $parsed = [];
        if ($query === '') {
            return $parsed;
        }

        foreach (explode('&', $query) as $pair) {
            if ($pair === '') {
                continue;
            }

            $parts = explode('=', $pair, 2);
            $parsed[rawurldecode($parts[0])][] = isset($parts[1]) ? rawurldecode($parts[1]) : '';
        }

        return $parsed;
    }
}
