from __future__ import annotations

import hashlib

import pytest
from pydantic import ValidationError

from payment_service.errors import InvalidIdempotencyKeyError
from payment_service.payments import validate_idempotency_key
from payment_service.schemas import PaymentCreate


def test_payment_fingerprint_is_canonical() -> None:
    first = PaymentCreate.model_validate(
        {
            "amount": "10",
            "currency": "USD",
            "description": "Order 42",
            "metadata": {"z": 2, "a": 1},
            "webhook_url": "https://hooks.example.test/payments",
        }
    )
    second = PaymentCreate.model_validate(
        {
            "amount": "10.00",
            "currency": "USD",
            "description": "Order 42",
            "metadata": {"a": 1, "z": 2},
            "webhook_url": "https://hooks.example.test/payments",
        }
    )
    canonical_json = (
        '{"amount":"10.00","currency":"USD","description":"Order 42",'
        '"metadata":{"a":1,"z":2},'
        '"webhook_url":"https://hooks.example.test/payments"}'
    )

    assert first.fingerprint() == second.fingerprint()
    assert first.fingerprint() == hashlib.sha256(canonical_json.encode()).hexdigest()


@pytest.mark.parametrize(
    ("changed_field", "changed_value"),
    [
        ("amount", "10.01"),
        ("currency", "EUR"),
        ("description", "Order 43"),
        ("metadata", {"a": 1, "z": 3}),
        ("webhook_url", "https://hooks.example.test/other"),
    ],
)
def test_payment_fingerprint_changes_with_request(
    changed_field: str,
    changed_value: object,
) -> None:
    payload: dict[str, object] = {
        "amount": "10.00",
        "currency": "USD",
        "description": "Order 42",
        "metadata": {"a": 1},
        "webhook_url": "https://hooks.example.test/payments",
    }
    baseline = PaymentCreate.model_validate(payload)
    payload[changed_field] = changed_value

    assert PaymentCreate.model_validate(payload).fingerprint() != baseline.fingerprint()


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("amount", "0"),
        ("amount", "-1"),
        ("amount", "1.001"),
        ("amount", "10000000000000000.00"),
        ("currency", "GBP"),
        ("description", ""),
        ("description", "x" * 1001),
        ("metadata", {"not_json": object()}),
        ("webhook_url", "ftp://hooks.example.test/payments"),
        ("webhook_url", "https://user:password@hooks.example.test/payments"),
    ],
)
def test_payment_create_rejects_invalid_fields(
    field_name: str,
    invalid_value: object,
) -> None:
    payload: dict[str, object] = {
        "amount": "10.00",
        "currency": "USD",
        "description": "Order 42",
        "metadata": {},
        "webhook_url": "https://hooks.example.test/payments",
    }
    payload[field_name] = invalid_value

    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(payload)


@pytest.mark.parametrize(
    "metadata",
    [
        {"value": float("nan")},
        {"nested": [1, {"value": float("inf")}]},
        {"nested": {"values": [0, float("-inf")]}},
    ],
)
def test_payment_create_rejects_non_finite_metadata_recursively(
    metadata: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(
            {
                "amount": "10.00",
                "currency": "USD",
                "description": "Order 42",
                "metadata": metadata,
                "webhook_url": "https://hooks.example.test/payments",
            }
        )


def test_payment_create_accepts_deeply_nested_finite_metadata() -> None:
    metadata = {
        "nested": {
            "values": [0, 1.25, -3.5, {"enabled": True, "optional": None}],
        }
    }

    payment = PaymentCreate.model_validate(
        {
            "amount": "10.00",
            "currency": "USD",
            "description": "Order 42",
            "metadata": metadata,
            "webhook_url": "https://hooks.example.test/payments",
        }
    )

    assert payment.metadata == metadata


@pytest.mark.parametrize("idempotency_key", ["payment-42", " ключ ", "x" * 255])
def test_validate_idempotency_key_accepts_supported_values(idempotency_key: str) -> None:
    validate_idempotency_key(idempotency_key)


@pytest.mark.parametrize("idempotency_key", ["", "   ", "line\nbreak", "x" * 256])
def test_validate_idempotency_key_rejects_unsupported_values(idempotency_key: str) -> None:
    with pytest.raises(InvalidIdempotencyKeyError):
        validate_idempotency_key(idempotency_key)
