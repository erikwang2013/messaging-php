<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Tests\Integration;

use Erikwang2013\Messaging\Config\ConnectionConfig;
use Erikwang2013\Messaging\Contract\DriverInterface;
use Erikwang2013\Messaging\Contract\Message;
use Erikwang2013\Messaging\Contract\ReceiveOptions;
use Erikwang2013\Messaging\Driver\Gcp\GcpPubSubFactory;
use Erikwang2013\Messaging\Exception\UnsupportedFeatureException;
use Erikwang2013\Messaging\Tests\Contract\DriverContractTestCase;
use GuzzleHttp\Client;
use GuzzleHttp\Exception\GuzzleException;
use PHPUnit\Framework\Attributes\Group;

/**
 * Runs against the Pub/Sub emulator from `docker compose --profile gcp up -d`.
 *
 * The contract passes one destination to both publish and receive, so each test
 * provisions a topic and a subscription sharing that resource name.
 */
#[Group('integration')]
final class PubSubTest extends DriverContractTestCase
{
    private DriverInterface $driver;
    private Client $http;
    private string $project;
    private string $baseUri;
    private ?string $provisioned = null;

    protected function setUp(): void
    {
        parent::setUp();

        $name = $this->baseDestination();
        $this->project = getenv('MESSAGING_GCP_PROJECT') ?: 'test-project';
        $this->baseUri = 'http://' . $this->emulatorHost() . '/v1/projects/' . rawurlencode($this->project);
        $this->http = new Client(['http_errors' => false, 'timeout' => 5.0]);

        try {
            $topic = $this->http->put($this->baseUri . '/topics/' . rawurlencode($name), [
                'json' => new \stdClass(),
                'timeout' => 2.0,
            ]);
            if ($topic->getStatusCode() >= 400 && $topic->getStatusCode() !== 409) {
                self::markTestSkipped('Pub/Sub emulator rejected topic creation: ' . (string) $topic->getBody());
            }

            $subscription = $this->http->put($this->baseUri . '/subscriptions/' . rawurlencode($name), [
                'json' => ['topic' => sprintf('projects/%s/topics/%s', $this->project, $name)],
                'timeout' => 2.0,
            ]);
            if ($subscription->getStatusCode() >= 400 && $subscription->getStatusCode() !== 409) {
                self::markTestSkipped('Pub/Sub emulator rejected subscription creation: ' . (string) $subscription->getBody());
            }
        } catch (GuzzleException $exception) {
            self::markTestSkipped('Pub/Sub emulator is not available: ' . $exception->getMessage());
        }

        $this->provisioned = $name;
    }

    protected function tearDown(): void
    {
        if ($this->provisioned !== null && isset($this->http)) {
            $name = rawurlencode($this->provisioned);
            try {
                $this->http->delete($this->baseUri . '/subscriptions/' . $name, ['timeout' => 2.0]);
                $this->http->delete($this->baseUri . '/topics/' . $name, ['timeout' => 2.0]);
            } catch (GuzzleException) {
                // Cleanup is best-effort; a missing emulator must not mask test results.
            }
        }

        parent::tearDown();
    }

    protected function createDriver(): DriverInterface
    {
        $config = ConnectionConfig::fromArray('gcp', [
            'driver' => 'gcp-pubsub',
            'project_id' => getenv('MESSAGING_GCP_PROJECT') ?: 'test-project',
            'emulator_host' => $this->emulatorHost(),
            'timeout' => 5.0,
        ]);

        $this->driver = (new GcpPubSubFactory())->create($config);

        return $this->driver;
    }

    public function test_delay_is_unsupported(): void
    {
        $this->expectException(UnsupportedFeatureException::class);

        $this->driver->publish($this->baseDestination(), new Message('payload', [], null, 100));
    }

    public function test_reject_without_requeue_discards_message(): void
    {
        $destination = $this->baseDestination();
        $this->driver->publish($destination, new Message('drop-me'));

        $message = $this->driver->receive($destination, new ReceiveOptions(1000));
        self::assertNotNull($message);
        $message->reject(false);

        self::assertNull($this->driver->receive($destination, new ReceiveOptions(100)));
    }

    public function test_attempts_are_always_one(): void
    {
        $destination = $this->baseDestination();
        $this->driver->publish($destination, new Message('attempts'));

        $first = $this->driver->receive($destination, new ReceiveOptions(1000));
        self::assertNotNull($first);
        self::assertSame(1, $first->attempts());
        $first->reject(true);

        $second = $this->driver->receive($destination, new ReceiveOptions(1000));
        self::assertNotNull($second);
        self::assertSame(1, $second->attempts());
        $second->ack();
    }

    private function emulatorHost(): string
    {
        return getenv('MESSAGING_PUBSUB_EMULATOR_HOST') ?: '127.0.0.1:8085';
    }

    /**
     * The contract keeps the destination private; provision the matching topic
     * and subscription from it instead of duplicating every contract test.
     */
    private function baseDestination(): string
    {
        $property = new \ReflectionProperty(DriverContractTestCase::class, 'destination');
        if (PHP_VERSION_ID < 80100) {
            // Private reflection needs explicit access only before PHP 8.1.
            $property->setAccessible(true);
        }

        $value = $property->getValue($this);
        if (!is_string($value)) {
            throw new \RuntimeException('Contract destination is not initialised.');
        }

        return $value;
    }
}
