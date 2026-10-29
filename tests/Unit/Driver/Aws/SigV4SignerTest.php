<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Unit\Driver\Aws;

use Erikwang2013\Messaging\Driver\Aws\SigV4Signer;
use PHPUnit\Framework\TestCase;

final class SigV4SignerTest extends TestCase
{
    private const EMPTY_HASH = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';

    /**
     * Official example from the IAM User Guide (signed request examples).
     */
    public function test_iam_list_users_official_vector(): void
    {
        $signer = new SigV4Signer('AKIDEXAMPLE', 'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY');

        $headers = $signer->sign(
            'GET',
            'https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08',
            ['Content-Type' => 'application/x-www-form-urlencoded; charset=utf-8'],
            '',
            'us-east-1',
            'iam',
            strtotime('2015-08-30T12:36:00Z'),
        );

        self::assertSame('iam.amazonaws.com', $headers['host']);
        self::assertSame('20150830T123600Z', $headers['x-amz-date']);
        self::assertSame(
            'AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, '
            . 'SignedHeaders=content-type;host;x-amz-date, '
            . 'Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7',
            $headers['authorization'],
        );
    }

    /**
     * Official "GET Object" example from the S3 Signature Version 4 documentation.
     */
    public function test_s3_get_object_official_vector(): void
    {
        $signer = new SigV4Signer('AKIAIOSFODNN7EXAMPLE', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY');

        $headers = $signer->sign(
            'GET',
            'https://examplebucket.s3.amazonaws.com/test.txt',
            ['Range' => 'bytes=0-9', 'x-amz-content-sha256' => self::EMPTY_HASH],
            '',
            'us-east-1',
            's3',
            strtotime('2013-05-24T00:00:00Z'),
        );

        self::assertSame(
            'AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, '
            . 'SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, '
            . 'Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41',
            $headers['authorization'],
        );
    }

    /**
     * Official "GET Bucket lifecycle" example: empty query value.
     */
    public function test_s3_lifecycle_official_vector(): void
    {
        $signer = new SigV4Signer('AKIAIOSFODNN7EXAMPLE', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY');

        $headers = $signer->sign(
            'GET',
            'https://examplebucket.s3.amazonaws.com/?lifecycle',
            ['x-amz-content-sha256' => self::EMPTY_HASH],
            '',
            'us-east-1',
            's3',
            strtotime('2013-05-24T00:00:00Z'),
        );

        self::assertSame(
            'AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, '
            . 'SignedHeaders=host;x-amz-content-sha256;x-amz-date, '
            . 'Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543',
            $headers['authorization'],
        );
    }

    /**
     * Official "GET Bucket (List Objects)" canonical request.
     */
    public function test_s3_list_objects_canonical_request_matches_official_hash(): void
    {
        $canonical = SigV4Signer::canonicalRequest(
            'GET',
            '/',
            ['max-keys' => '2', 'prefix' => 'J'],
            ['host' => 'examplebucket.s3.amazonaws.com', 'x-amz-content-sha256' => self::EMPTY_HASH, 'x-amz-date' => '20130524T000000Z'],
            self::EMPTY_HASH,
        );

        self::assertSame(
            "GET\n/\nmax-keys=2&prefix=J\n"
            . "host:examplebucket.s3.amazonaws.com\nx-amz-content-sha256:" . self::EMPTY_HASH . "\nx-amz-date:20130524T000000Z\n\n"
            . "host;x-amz-content-sha256;x-amz-date\n" . self::EMPTY_HASH,
            $canonical,
        );
        self::assertSame(
            'df57d21db20da04d7fa30298dd4488ba3a2b47ca3a489c74750e0f1e7df1b9b7',
            hash('sha256', $canonical),
        );
    }

    public function test_session_token_is_signed(): void
    {
        $signer = new SigV4Signer('AKID', 'SECRET', 'session-token');

        $headers = $signer->sign('POST', 'https://sqs.us-east-1.amazonaws.com/', [], '{}', 'us-east-1', 'sqs', 1600000000);

        self::assertSame('session-token', $headers['x-amz-security-token']);
        self::assertStringContainsString('SignedHeaders=host;x-amz-date;x-amz-security-token', $headers['authorization']);
    }

    public function test_query_values_are_sorted_and_encoded(): void
    {
        self::assertSame(
            'a=1&b=2&b=3&empty=&q=a%20b%2F%C3%BC~',
            SigV4Signer::canonicalQuery([
                'b' => ['3', '2'],
                'empty' => '',
                'a' => '1',
                'q' => 'a b/ü~',
            ]),
        );
    }

    public function test_path_segments_are_encoded(): void
    {
        $canonical = SigV4Signer::canonicalRequest('GET', '/a b/ü', [], ['host' => 'h'], self::EMPTY_HASH);

        self::assertStringStartsWith("GET\n/a%20b/%C3%BC\n\nhost:h\n\nhost\n", $canonical);
        self::assertSame('a/b', SigV4Signer::uriEncode('a/b', false));
    }

    public function test_invalid_url_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new SigV4Signer('AKID', 'SECRET'))->sign('GET', '/relative', [], '', 'us-east-1', 'sqs');
    }
}
