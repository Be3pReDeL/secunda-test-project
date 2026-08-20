from enum import StrEnum


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


PAYMENT_CREATED_EVENT = "payment.created.v1"
PAYMENTS_EXCHANGE_NAME = "payments"
PAYMENTS_NEW_QUEUE_NAME = "payments.new"
PAYMENTS_NEW_ROUTING_KEY = "payments.new"
PAYMENTS_RETRY_EXCHANGE_NAME = "payments.retry"
PAYMENTS_DEAD_LETTER_EXCHANGE_NAME = "payments.dlx"
PAYMENTS_DEAD_LETTER_QUEUE_NAME = "payments.dlq"
PAYMENTS_DEAD_LETTER_ROUTING_KEY = "payments.dead"
RETRY_ATTEMPT_HEADER = "x-processing-attempt"
MAX_PROCESSING_ATTEMPTS = 3
