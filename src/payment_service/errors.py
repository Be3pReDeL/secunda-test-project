class IdempotencyConflictError(Exception):
    """The same idempotency key was used for a different request."""


class InvalidIdempotencyKeyError(ValueError):
    """The idempotency key is outside the supported contract."""


class PaymentNotFoundError(Exception):
    """The requested payment does not exist."""


class WebhookDeliveryError(Exception):
    """The webhook could not be delivered."""
