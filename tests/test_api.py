from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from typing import Any

import pytest
import pytest_asyncio
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import Settings, get_settings
from payment_service.database import get_session
from payment_service.main import create_app
from payment_service.models import OutboxEvent, Payment
from payment_service.security import require_api_key

TEST_API_KEY = "test-api-key-000000000000"
API_HEADERS = {
    "X-API-Key": TEST_API_KEY,
    "Idempotency-Key": "payment-request-42",
}
PAYMENT_PAYLOAD: dict[str, Any] = {
    "amount": "125.50",
    "currency": "RUB",
    "description": "Test order",
    "metadata": {"customer": "customer-42", "items": 2},
    "webhook_url": "https://hooks.example.test/payments",
}


@pytest_asyncio.fixture
async def api_client(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> AsyncIterator[AsyncClient]:
    application = create_app(start_outbox_relay=False)

    async def override_session() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    application.dependency_overrides[get_session] = override_session
    application.dependency_overrides[get_settings] = lambda: settings
    async with AsyncClient(
        transport=ASGITransport(app=application),
        base_url="http://testserver",
    ) as client:
        yield client
    application.dependency_overrides.clear()


async def row_count(
    session_factory: async_sessionmaker[AsyncSession],
    model: type[Payment] | type[OutboxEvent],
) -> int:
    async with session_factory() as session:
        count = await session.scalar(select(func.count()).select_from(model))
    assert count is not None
    return count


def normalized_timestamp(timestamp: str) -> datetime:
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"X-API-Key": "wrong-api-key"},
    ],
)
async def test_api_rejects_missing_or_invalid_api_key(
    api_client: AsyncClient,
    headers: dict[str, str],
) -> None:
    response = await api_client.get("/health", headers=headers)

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid or missing API key"}
    assert response.headers["www-authenticate"] == "ApiKey"


async def test_api_key_dependency_rejects_unicode_without_type_error(
    settings: Settings,
) -> None:
    with pytest.raises(HTTPException) as unauthorized:
        await require_api_key(settings=settings, x_api_key="неверный-api-key")

    assert unauthorized.value.status_code == 401
    assert unauthorized.value.detail == "Invalid or missing API key"


async def test_health_endpoint_checks_database(api_client: AsyncClient) -> None:
    response = await api_client.get("/health", headers={"X-API-Key": TEST_API_KEY})

    assert response.status_code == 204
    assert response.content == b""


async def test_post_and_get_payment_persist_atomic_outbox(
    api_client: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    create_response = await api_client.post(
        "/api/v1/payments",
        headers=API_HEADERS,
        json=PAYMENT_PAYLOAD,
    )

    assert create_response.status_code == 202
    accepted = create_response.json()
    assert accepted["status"] == "pending"
    assert accepted["payment_id"]
    assert accepted["created_at"]

    get_response = await api_client.get(
        f"/api/v1/payments/{accepted['payment_id']}",
        headers={"X-API-Key": TEST_API_KEY},
    )

    assert get_response.status_code == 200
    payment_details = get_response.json()
    persisted_created_at = payment_details.pop("created_at")
    assert payment_details == {
        "payment_id": accepted["payment_id"],
        "amount": "125.50",
        "currency": "RUB",
        "description": "Test order",
        "metadata": {"customer": "customer-42", "items": 2},
        "status": "pending",
        "webhook_url": "https://hooks.example.test/payments",
        "processed_at": None,
    }
    assert normalized_timestamp(persisted_created_at) == normalized_timestamp(
        accepted["created_at"]
    )

    async with session_factory() as session:
        outbox_event = await session.scalar(select(OutboxEvent))
    assert outbox_event is not None
    assert str(outbox_event.aggregate_id) == accepted["payment_id"]
    assert str(outbox_event.id) == outbox_event.payload["event_id"]
    assert outbox_event.payload["payment_id"] == accepted["payment_id"]
    assert outbox_event.payload["event_type"] == "payment.created.v1"


async def test_post_is_idempotent_and_detects_fingerprint_conflict(
    api_client: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first_response = await api_client.post(
        "/api/v1/payments",
        headers=API_HEADERS,
        json=PAYMENT_PAYLOAD,
    )
    repeated_response = await api_client.post(
        "/api/v1/payments",
        headers=API_HEADERS,
        json=PAYMENT_PAYLOAD,
    )
    changed_payload = {**PAYMENT_PAYLOAD, "description": "Different order"}
    conflict_response = await api_client.post(
        "/api/v1/payments",
        headers=API_HEADERS,
        json=changed_payload,
    )

    assert first_response.status_code == 202
    assert repeated_response.status_code == 202
    first_accepted = first_response.json()
    repeated_accepted = repeated_response.json()
    assert repeated_accepted["payment_id"] == first_accepted["payment_id"]
    assert repeated_accepted["status"] == first_accepted["status"]
    assert normalized_timestamp(repeated_accepted["created_at"]) == normalized_timestamp(
        first_accepted["created_at"]
    )
    assert conflict_response.status_code == 409
    assert conflict_response.json() == {
        "detail": "Idempotency-Key was already used with a different request"
    }
    assert await row_count(session_factory, Payment) == 1
    assert await row_count(session_factory, OutboxEvent) == 1


async def test_post_rolls_back_payment_when_outbox_insert_fails(
    api_client: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    def fail_outbox_insert(*_: object) -> None:
        raise RuntimeError("outbox insert failed")

    listener: Callable[..., None] = fail_outbox_insert
    event.listen(OutboxEvent, "before_insert", listener)
    try:
        with pytest.raises(RuntimeError, match="outbox insert failed"):
            await api_client.post(
                "/api/v1/payments",
                headers=API_HEADERS,
                json=PAYMENT_PAYLOAD,
            )
    finally:
        event.remove(OutboxEvent, "before_insert", listener)

    assert await row_count(session_factory, Payment) == 0
    assert await row_count(session_factory, OutboxEvent) == 0


async def test_api_reports_invalid_key_and_missing_payment(api_client: AsyncClient) -> None:
    invalid_key_response = await api_client.post(
        "/api/v1/payments",
        headers={
            "X-API-Key": TEST_API_KEY,
            "Idempotency-Key": "   ",
        },
        json=PAYMENT_PAYLOAD,
    )
    missing_response = await api_client.get(
        "/api/v1/payments/00000000-0000-0000-0000-000000000042",
        headers={"X-API-Key": TEST_API_KEY},
    )

    assert invalid_key_response.status_code == 422
    assert invalid_key_response.json() == {
        "detail": "Idempotency-Key must contain 1-255 printable characters"
    }
    assert missing_response.status_code == 404
    assert missing_response.json() == {"detail": "Payment not found"}


@pytest.mark.parametrize("non_finite_number", ["NaN", "Infinity", "-Infinity"])
async def test_post_rejects_non_finite_metadata_without_reflecting_input(
    api_client: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    non_finite_number: str,
) -> None:
    raw_payload = (
        '{"amount":"1.00","currency":"RUB","description":"invalid",'
        f'"metadata":{{"nested":[{non_finite_number}]}},'
        '"webhook_url":"https://hooks.example.test/payment"}'
    )

    response = await api_client.post(
        "/api/v1/payments",
        headers={
            **API_HEADERS,
            "Idempotency-Key": f"non-finite-{non_finite_number}",
            "Content-Type": "application/json",
        },
        content=raw_payload,
    )

    assert response.status_code == 422
    validation_error = response.json()["detail"][0]
    assert validation_error == {
        "type": "value_error",
        "loc": ["body", "metadata"],
        "msg": "Value error, metadata must contain only finite numbers",
    }
    assert await row_count(session_factory, Payment) == 0
    assert await row_count(session_factory, OutboxEvent) == 0


def test_application_disables_public_schema_routes() -> None:
    application: FastAPI = create_app(start_outbox_relay=False)

    assert application.openapi_url is None
    assert application.docs_url is None
    assert application.redoc_url is None
