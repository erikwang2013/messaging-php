<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Driver\Redis\Resp;

use Erikwang2013\Messaging\Exception\AuthenticationException;
use Erikwang2013\Messaging\Exception\ProtocolException;
use Erikwang2013\Messaging\Support\Socket;

final class RespClient
{
    private readonly RespParser $parser;

    private function __construct(
        private readonly Socket $socket,
        private readonly float $commandTimeout,
    ) {
        $this->parser = new RespParser(
            fn (): string => $this->socket->readLine(),
            fn (int $length): string => $this->socket->read($length),
        );
    }

    public static function connect(
        string $host,
        int $port,
        ?string $password,
        int $database,
        float $timeout,
    ): self {
        $client = new self(Socket::connect($host, $port, $timeout), $timeout);

        if ($password !== null && $password !== '') {
            try {
                $client->command('AUTH', $password);
            } catch (ProtocolException $exception) {
                throw new AuthenticationException($exception->getMessage(), 0, $exception);
            }
        }

        if ($database !== 0) {
            $client->command('SELECT', (string) $database);
        }

        return $client;
    }

    public function command(string|int ...$arguments): mixed
    {
        $this->socket->write(RespEncoder::encode($arguments));

        try {
            return $this->parser->parse();
        } catch (ProtocolException $exception) {
            $message = $exception->getMessage();
            if (str_starts_with($message, 'NOAUTH') || str_starts_with($message, 'WRONGPASS')) {
                throw new AuthenticationException($message, 0, $exception);
            }

            throw $exception;
        }
    }

    /**
     * Socket reads must outlast the server-side BLOCK; a receive with BLOCK 0
     * uses a large cap because stream_set_timeout(0) does not block forever.
     */
    public function setReadTimeout(float $seconds): void
    {
        $whole = (int) $seconds;
        stream_set_timeout($this->socket->stream(), $whole, (int) (($seconds - $whole) * 1_000_000));
    }

    public function resetReadTimeout(): void
    {
        $this->setReadTimeout($this->commandTimeout);
    }

    public function close(): void
    {
        $this->socket->close();
    }
}
