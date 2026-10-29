<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Aws;

/**
 * Maps message headers to and from the AWS message attribute shape.
 *
 * Scalars travel as String attributes; anything else is sent as Binary
 * (Stringable values as their raw string, other values as JSON).
 */
final class MessageAttributes
{
    private const NAME_PATTERN = '~^[A-Za-z0-9_.-]{1,256}$~';

    /**
     * @param array<string, mixed> $headers
     * @return array<string, array{DataType: string, StringValue?: string, BinaryValue?: string}>
     */
    public static function encode(array $headers): array
    {
        if (count($headers) > 10) {
            throw new \InvalidArgumentException('AWS supports at most 10 message attributes.');
        }

        $attributes = [];
        foreach ($headers as $name => $value) {
            self::assertName($name);

            if (is_string($value) && preg_match('//u', $value) !== 1) {
                $attributes[$name] = [
                    'DataType' => 'Binary',
                    'BinaryValue' => base64_encode($value),
                ];

                continue;
            }

            if (is_scalar($value) || $value === null) {
                $attributes[$name] = [
                    'DataType' => 'String',
                    'StringValue' => is_bool($value) ? ($value ? 'true' : 'false') : (string) $value,
                ];

                continue;
            }

            $attributes[$name] = [
                'DataType' => 'Binary',
                'BinaryValue' => base64_encode($value instanceof \Stringable
                    ? (string) $value
                    : (string) json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES)),
            ];
        }

        return $attributes;
    }

    /**
     * @param array<string, mixed> $attributes MessageAttributes member of a receive response.
     * @return array<string, mixed>
     */
    public static function decode(array $attributes): array
    {
        $headers = [];
        foreach ($attributes as $name => $attribute) {
            if (!is_array($attribute)) {
                continue;
            }

            if (isset($attribute['BinaryValue']) && is_string($attribute['BinaryValue'])) {
                $decoded = base64_decode($attribute['BinaryValue'], true);
                $headers[$name] = $decoded === false ? '' : $decoded;

                continue;
            }

            $headers[$name] = isset($attribute['StringValue']) && is_scalar($attribute['StringValue'])
                ? (string) $attribute['StringValue']
                : (isset($attribute['NumberValue']) && is_scalar($attribute['NumberValue']) ? (string) $attribute['NumberValue'] : '');
        }

        return $headers;
    }

    private static function assertName(mixed $name): void
    {
        if (
            !is_string($name)
            || preg_match(self::NAME_PATTERN, $name) !== 1
            || $name[0] === '.'
            || str_ends_with($name, '.')
            || preg_match('/\A(?:aws|amazon)\./i', $name) === 1
        ) {
            throw new \InvalidArgumentException(sprintf(
                'Invalid AWS message attribute name [%s]: use 1-256 characters from A-Z a-z 0-9 _ . - and do not start or end with a dot.',
                is_string($name) ? $name : get_debug_type($name),
            ));
        }
    }
}
