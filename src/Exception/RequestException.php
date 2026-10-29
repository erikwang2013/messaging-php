<?php

declare(strict_types=1);

namespace Erikwang2013\Messaging\Exception;

class RequestException extends MessagingException
{
    public function __construct(
        string $message,
        private ?int $statusCode = null,
        private ?string $responseBody = null,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, 0, $previous);
    }

    public function statusCode(): ?int
    {
        return $this->statusCode;
    }

    public function responseBody(): ?string
    {
        return $this->responseBody;
    }
}
