from __future__ import annotations

import os
from collections.abc import AsyncIterator

os.environ.update(
    {
        "PAYMENTS_API_KEY": "test-api-key-000000000000",
        "PAYMENTS_DATABASE_URL": "sqlite+aiosqlite://",
        "PAYMENTS_RABBITMQ_URL": "amqp://guest:guest@localhost:5672/",
        "PAYMENTS_GATEWAY_MIN_DELAY_SECONDS": "0",
        "PAYMENTS_GATEWAY_MAX_DELAY_SECONDS": "0",
        "PAYMENTS_RETRY_BASE_DELAY_SECONDS": "0.01",
        "PAYMENTS_WEBHOOK_TIMEOUT_SECONDS": "0.1",
        "PAYMENTS_BROKER_PUBLISH_TIMEOUT_SECONDS": "0.1",
        "PAYMENTS_BROKER_RECONNECT_INTERVAL_SECONDS": "0.01",
        "PAYMENTS_OUTBOX_POLL_INTERVAL_SECONDS": "0.01",
        "PAYMENTS_OUTBOX_LEASE_SECONDS": "1",
        "PAYMENTS_OUTBOX_MAX_BACKOFF_SECONDS": "1",
    }
)

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from payment_service.config import Settings
from payment_service.models import Base
from payment_service.schemas import PaymentCreate

TEST_API_KEY = "test-api-key-000000000000"


@pytest.fixture
def settings() -> Settings:
    return Settings(_env_file=None)


@pytest.fixture
def payment_data() -> PaymentCreate:
    return PaymentCreate.model_validate(
        {
            "amount": "125.50",
            "currency": "RUB",
            "description": "Test order",
            "metadata": {"customer": "customer-42", "items": 2},
            "webhook_url": "https://hooks.example.test/payments",
        }
    )


@pytest_asyncio.fixture
async def session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        poolclass=StaticPool,
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        await engine.dispose()
