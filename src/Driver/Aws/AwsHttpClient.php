<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Aws;

use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ConnectionException;
use Erikwang2013\Messaging\Exception\MessagingException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Exception\RequestException;
use Erikwang2013\Messaging\Exception\TimeoutException;
use GuzzleHttp\Client;
use GuzzleHttp\ClientInterface;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\TransferException;
use Psr\Log\LoggerInterface;

/**
 * Signs and sends AWS API requests through Guzzle.
 */
final class AwsHttpClient
{
    private ClientInterface $http;
    private SigV4Signer $signer;
    private AwsConfig $config;
    private ?LoggerInterface $logger;

    public function __construct(AwsConfig $config, ?LoggerInterface $logger = null, ?ClientInterface $http = null)
    {
        $this->config = $config;
        $this->logger = $logger;
        $this->signer = new SigV4Signer($config->accessKey, $config->secretKey, $config->sessionToken);
        $this->http = $http ?? new Client([
            'timeout' => $config->timeout,
            'connect_timeout' => min(10.0, $config->timeout),
        ]);
    }

    /**
     * Regional endpoint (or the configured AWS-compatible endpoint override) with a trailing slash.
     */
    public function endpoint(string $service): string
    {
        $base = $this->config->endpoint ?? sprintf('https://%s.%s.amazonaws.com', $service, $this->config->region);

        return rtrim($base, '/') . '/';
    }

    public function timeout(): float
    {
        return $this->config->timeout;
    }

    /**
     * @param array<string, string> $headers
     */
    public function request(string $service, string $method, string $url, array $headers, string $body = '', ?float $timeout = null): string
    {
        $timeout ??= $this->config->timeout;
        $signed = $this->signer->sign($method, $url, $headers, $body, $this->config->region, $service);

        try {
            $response = $this->http->request($method, $url, [
                'headers' => $signed,
                'body' => $body,
                'timeout' => $timeout,
                'connect_timeout' => min(10.0, $timeout),
                'http_errors' => false,
                'allow_redirects' => false,
            ]);
        } catch (ConnectException $exception) {
            throw $this->connectionError($service, $exception);
        } catch (TransferException $exception) {
            throw new ConnectionException(sprintf('AWS %s request failed: %s', $service, $exception->getMessage()), 0, $exception);
        }

        $status = $response->getStatusCode();
        $payload = (string) $response->getBody();
        $this->logger?->debug('AWS request completed', [
            'service' => $service,
            'method' => $method,
            'status' => $status,
        ]);

        if ($status >= 400) {
            throw $this->requestError($service, $status, $payload);
        }

        return $payload;
    }

    /**
     * Calls a JSON-protocol operation (SQS, Kinesis).
     *
     * @param array<string, mixed> $payload
     * @return array<string, mixed>
     */
    public function json(
        string $service,
        string $target,
        string $url,
        array $payload,
        ?float $timeout = null,
        string $jsonVersion = '1.0',
    ): array {
        $body = json_encode($payload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
        $raw = $this->request($service, 'POST', $url, [
            'Content-Type' => 'application/x-amz-json-' . $jsonVersion,
            'X-Amz-Target' => $target,
        ], $body, $timeout);

        if (trim($raw) === '') {
            return [];
        }

        $decoded = json_decode($raw, true);
        if (!is_array($decoded)) {
            throw new ProtocolException(sprintf('Malformed JSON response from AWS %s: %s', $service, $raw));
        }

        /** @var array<string, mixed> $decoded */
        return $decoded;
    }

    /**
     * Calls a Query-protocol operation (SNS) and returns the raw XML response.
     *
     * @param array<string, mixed> $params
     */
    public function query(string $service, string $url, array $params, ?float $timeout = null): string
    {
        return $this->request($service, 'POST', $url, [
            'Content-Type' => 'application/x-www-form-urlencoded; charset=utf-8',
        ], http_build_query($params, '', '&', PHP_QUERY_RFC3986), $timeout);
    }

    private function connectionError(string $service, ConnectException $exception): ConnectionException
    {
        // cURL error 28 is CURLE_OPERATION_TIMEOUTED.
        $errno = $exception->getHandlerContext()['errno'] ?? null;
        if ($errno === 28 || stripos($exception->getMessage(), 'timed out') !== false) {
            return new TimeoutException(sprintf('AWS %s request timed out: %s', $service, $exception->getMessage()), 0, $exception);
        }

        return new ConnectionException(sprintf('AWS %s connection failed: %s', $service, $exception->getMessage()), 0, $exception);
    }

    private function requestError(string $service, int $status, string $body): MessagingException
    {
        [$code, $message] = self::parseError($body);
        $summary = sprintf(
            'AWS %s request failed with HTTP %d%s%s',
            $service,
            $status,
            $code === null ? '' : ' [' . $code . ']',
            $message === null ? ($body === '' ? '' : ': ' . $body) : ': ' . $message,
        );

        if ($status === 401 || $status === 403) {
            return new AuthenticationException($summary);
        }

        return new RequestException($summary, $status, $body);
    }

    /**
     * @return array{0: ?string, 1: ?string}
     */
    private static function parseError(string $body): array
    {
        $decoded = json_decode($body, true);
        if (is_array($decoded)) {
            $code = $decoded['__type'] ?? $decoded['code'] ?? null;
            if (is_string($code)) {
                $separator = strrpos($code, '#');
                if ($separator !== false) {
                    $code = substr($code, $separator + 1);
                }
            } else {
                $code = null;
            }

            $message = $decoded['message'] ?? $decoded['Message'] ?? null;

            return [$code, is_string($message) ? $message : null];
        }

        return [self::xmlTag($body, 'Code'), self::xmlTag($body, 'Message')];
    }

    private static function xmlTag(string $xml, string $tag): ?string
    {
        $pattern = '~<' . preg_quote($tag, '~') . '>(.*?)</' . preg_quote($tag, '~') . '>~s';
        if (preg_match($pattern, $xml, $matches) !== 1) {
            return null;
        }

        return html_entity_decode($matches[1], ENT_QUOTES | ENT_XML1, 'UTF-8');
    }
}
