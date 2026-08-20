from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.domain import (
    MAX_PROCESSING_ATTEMPTS,
    PAYMENT_CREATED_EVENT,
    PaymentStatus,
)
from payment_service.errors import PaymentNotFoundError
from payment_service.models import Payment
from payment_service.processor import PaymentProcessor, RetryableProcessingError
from payment_service.schemas import PaymentCreatedEvent, WebhookPayload

WEBHOOK_TIMEOUT_SECONDS = 1.0


class FakeGateway:
    def __init__(self, outcome: PaymentStatus) -> None:
        self.outcome = outcome
        self.calls: list[uuid.UUID] = []

    async def process(self, payment_id: uuid.UUID) -> PaymentStatus:
        self.calls.append(payment_id)
        return self.outcome


class FailingGateway:
    def __init__(self) -> None:
        self.calls: list[uuid.UUID] = []

    async def process(self, payment_id: uuid.UUID) -> PaymentStatus:
        self.calls.append(payment_id)
        raise TimeoutError("gateway unavailable")


class FakeWebhookSender:
    def __init__(self, *, failures: int = 0) -> None:
        self.remaining_failures = failures
        self.calls: list[tuple[str, WebhookPayload, uuid.UUID]] = []

    async def send(
        self,
        webhook_url: str,
        payload: WebhookPayload,
        *,
        event_id: uuid.UUID,
    ) -> None:
        self.calls.append((webhook_url, payload, event_id))
        if self.remaining_failures:
            self.remaining_failures -= 1
            raise ConnectionError("webhook unavailable")


class HangingWebhookSender:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls: list[tuple[str, WebhookPayload, uuid.UUID]] = []

    async def send(
        self,
        webhook_url: str,
        payload: WebhookPayload,
        *,
        event_id: uuid.UUID,
    ) -> None:
        self.calls.append((webhook_url, payload, event_id))
        self.started.set()
        await self.release.wait()


async def add_payment(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    status: PaymentStatus = PaymentStatus.PENDING,
    webhook_attempts: int = 0,
    webhook_last_error: str | None = None,
    webhook_delivered_at: datetime | None = None,
) -> Payment:
    payment_id = uuid.uuid4()
    processed_at = None if status is PaymentStatus.PENDING else datetime.now(UTC)
    payment = Payment(
        id=payment_id,
        amount="42.50",
        currency="USD",
        description="Processor test payment",
        metadata_={},
        status=status.value,
        idempotency_key=str(payment_id),
        request_fingerprint="f" * 64,
        webhook_url="https://hooks.example.test/payments",
        webhook_attempts=webhook_attempts,
        webhook_last_error=webhook_last_error,
        created_at=datetime.now(UTC),
        processed_at=processed_at,
        webhook_delivered_at=webhook_delivered_at,
    )
    async with session_factory.begin() as session:
        session.add(payment)
    return payment


def payment_event(payment_id: uuid.UUID) -> PaymentCreatedEvent:
    return PaymentCreatedEvent(
        event_id=uuid.uuid4(),
        payment_id=payment_id,
        event_type=PAYMENT_CREATED_EVENT,
        occurred_at=datetime.now(UTC),
    )


def build_processor(
    session_factory: async_sessionmaker[AsyncSession],
    gateway: FakeGateway | FailingGateway,
    webhook_sender: FakeWebhookSender | HangingWebhookSender,
    *,
    webhook_timeout_seconds: float = WEBHOOK_TIMEOUT_SECONDS,
) -> PaymentProcessor:
    return PaymentProcessor(
        session_factory=session_factory,
        gateway=gateway,
        webhook_sender=webhook_sender,
        webhook_timeout_seconds=webhook_timeout_seconds,
    )


async def test_processor_completes_payment_and_delivers_first_attempt(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await add_payment(session_factory)
    event = payment_event(payment.id)
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)
    webhook_sender = FakeWebhookSender()
    processor = build_processor(session_factory, gateway, webhook_sender)

    await processor.process(event, delivery_attempt=1)

    assert gateway.calls == [payment.id]
    assert len(webhook_sender.calls) == 1
    webhook_url, payload, event_id = webhook_sender.calls[0]
    assert webhook_url == "https://hooks.example.test/payments"
    assert event_id == event.event_id
    assert payload.event_id == event.event_id
    assert payload.payment_id == payment.id
    assert payload.status is PaymentStatus.SUCCEEDED
    async with session_factory() as session:
        persisted = await session.get(Payment, payment.id)
    assert persisted is not None
    assert persisted.status == PaymentStatus.SUCCEEDED.value
    assert persisted.processed_at is not None
    assert persisted.webhook_attempts == 1
    assert persisted.webhook_delivered_at is not None
    assert persisted.webhook_last_error is None


async def test_processor_rejects_unknown_payment(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)
    webhook_sender = FakeWebhookSender()
    processor = build_processor(session_factory, gateway, webhook_sender)

    with pytest.raises(PaymentNotFoundError):
        await processor.process(payment_event(uuid.uuid4()), delivery_attempt=1)

    assert gateway.calls == []
    assert webhook_sender.calls == []


async def test_processor_leaves_payment_pending_when_gateway_raises(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await add_payment(session_factory)
    gateway = FailingGateway()
    webhook_sender = FakeWebhookSender()
    processor = build_processor(session_factory, gateway, webhook_sender)

    with pytest.raises(TimeoutError, match="gateway unavailable"):
        await processor.process(payment_event(payment.id), delivery_attempt=1)

    assert gateway.calls == [payment.id]
    assert webhook_sender.calls == []
    async with session_factory() as session:
        persisted = await session.get(Payment, payment.id)
    assert persisted is not None
    assert persisted.status == PaymentStatus.PENDING.value
    assert persisted.processed_at is None
    assert persisted.webhook_attempts == 0


async def test_processor_delivers_failed_gateway_outcome(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await add_payment(session_factory)
    event = payment_event(payment.id)
    gateway = FakeGateway(PaymentStatus.FAILED)
    webhook_sender = FakeWebhookSender()
    processor = build_processor(session_factory, gateway, webhook_sender)

    await processor.process(event, delivery_attempt=1)

    assert webhook_sender.calls[0][1].status is PaymentStatus.FAILED
    async with session_factory() as session:
        persisted = await session.get(Payment, payment.id)
    assert persisted is not None
    assert persisted.status == PaymentStatus.FAILED.value


async def test_processor_retries_webhook_without_reprocessing_gateway(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await add_payment(session_factory)
    event = payment_event(payment.id)
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)
    webhook_sender = FakeWebhookSender(failures=1)
    processor = build_processor(session_factory, gateway, webhook_sender)

    with pytest.raises(RetryableProcessingError) as first_error:
        await processor.process(event, delivery_attempt=1)
    await processor.process(event, delivery_attempt=2)

    assert first_error.value.attempt == 1
    assert gateway.calls == [payment.id]
    assert len(webhook_sender.calls) == 2
    async with session_factory() as session:
        persisted = await session.get(Payment, payment.id)
    assert persisted is not None
    assert persisted.webhook_attempts == 2
    assert persisted.webhook_delivered_at is not None
    assert persisted.webhook_last_error is None


async def test_processor_persists_attempts_one_through_three(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await add_payment(session_factory)
    event = payment_event(payment.id)
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)
    webhook_sender = FakeWebhookSender(failures=MAX_PROCESSING_ATTEMPTS + 1)
    processor = build_processor(session_factory, gateway, webhook_sender)

    for delivery_attempt in range(1, MAX_PROCESSING_ATTEMPTS + 1):
        with pytest.raises(RetryableProcessingError) as processing_error:
            await processor.process(event, delivery_attempt=delivery_attempt)
        assert processing_error.value.attempt == delivery_attempt
        async with session_factory() as session:
            persisted_attempt = await session.get(Payment, payment.id)
        assert persisted_attempt is not None
        assert persisted_attempt.webhook_attempts == delivery_attempt

    with pytest.raises(RetryableProcessingError) as repeated_error:
        await processor.process(event, delivery_attempt=MAX_PROCESSING_ATTEMPTS)

    assert repeated_error.value.attempt == MAX_PROCESSING_ATTEMPTS
    assert gateway.calls == [payment.id]
    assert len(webhook_sender.calls) == MAX_PROCESSING_ATTEMPTS
    async with session_factory() as session:
        persisted = await session.get(Payment, payment.id)
    assert persisted is not None
    assert persisted.webhook_delivered_at is None
    assert persisted.webhook_last_error == "ConnectionError"


@pytest.mark.parametrize("redelivery_attempt", [1, 2])
async def test_processor_recovers_retry_from_same_or_older_redelivery(
    session_factory: async_sessionmaker[AsyncSession],
    redelivery_attempt: int,
) -> None:
    payment = await add_payment(
        session_factory,
        status=PaymentStatus.SUCCEEDED,
        webhook_attempts=2,
        webhook_last_error="TimeoutError",
    )
    gateway = FakeGateway(PaymentStatus.FAILED)
    webhook_sender = FakeWebhookSender()
    processor = build_processor(session_factory, gateway, webhook_sender)

    with pytest.raises(RetryableProcessingError) as retry_error:
        await processor.process(
            payment_event(payment.id),
            delivery_attempt=redelivery_attempt,
        )

    assert retry_error.value.attempt == 2
    assert gateway.calls == []
    assert webhook_sender.calls == []
    async with session_factory() as session:
        persisted = await session.get(Payment, payment.id)
    assert persisted is not None
    assert persisted.webhook_attempts == 2
    assert persisted.webhook_last_error == "TimeoutError"
    assert persisted.webhook_delivered_at is None


@pytest.mark.parametrize("redelivery_attempt", [1, 2, 3])
async def test_processor_delivered_redelivery_is_noop(
    session_factory: async_sessionmaker[AsyncSession],
    redelivery_attempt: int,
) -> None:
    delivered_at = datetime.now(UTC)
    payment = await add_payment(
        session_factory,
        status=PaymentStatus.SUCCEEDED,
        webhook_attempts=2,
        webhook_delivered_at=delivered_at,
    )
    gateway = FakeGateway(PaymentStatus.FAILED)
    webhook_sender = FakeWebhookSender()
    processor = build_processor(session_factory, gateway, webhook_sender)

    await processor.process(
        payment_event(payment.id),
        delivery_attempt=redelivery_attempt,
    )

    assert gateway.calls == []
    assert webhook_sender.calls == []
    async with session_factory() as session:
        persisted = await session.get(Payment, payment.id)
    assert persisted is not None
    assert persisted.webhook_attempts == 2
    assert persisted.webhook_delivered_at is not None


async def test_processor_cancellation_rolls_back_hanging_webhook_attempt(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await add_payment(session_factory)
    event = payment_event(payment.id)
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)
    webhook_sender = HangingWebhookSender()
    processor = build_processor(
        session_factory,
        gateway,
        webhook_sender,
        webhook_timeout_seconds=60,
    )
    processing_task = asyncio.create_task(
        processor.process(event, delivery_attempt=1),
    )
    await asyncio.wait_for(webhook_sender.started.wait(), timeout=1)

    processing_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await processing_task

    assert len(webhook_sender.calls) == 1
    async with session_factory() as session:
        persisted = await session.get(Payment, payment.id)
    assert persisted is not None
    assert persisted.status == PaymentStatus.SUCCEEDED.value
    assert persisted.webhook_attempts == 0
    assert persisted.webhook_last_error is None
    assert persisted.webhook_delivered_at is None


async def test_processor_timeout_commits_failed_attempt(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await add_payment(session_factory)
    event = payment_event(payment.id)
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)
    webhook_sender = HangingWebhookSender()
    processor = build_processor(
        session_factory,
        gateway,
        webhook_sender,
        webhook_timeout_seconds=0.01,
    )

    with pytest.raises(RetryableProcessingError) as processing_error:
        await processor.process(event, delivery_attempt=1)

    assert processing_error.value.attempt == 1
    assert isinstance(processing_error.value.__cause__, TimeoutError)
    assert len(webhook_sender.calls) == 1
    async with session_factory() as session:
        persisted = await session.get(Payment, payment.id)
    assert persisted is not None
    assert persisted.webhook_attempts == 1
    assert persisted.webhook_last_error == "TimeoutError"
    assert persisted.webhook_delivered_at is None


@pytest.mark.parametrize("delivery_attempt", [0, MAX_PROCESSING_ATTEMPTS + 1])
async def test_processor_rejects_invalid_delivery_attempt(
    session_factory: async_sessionmaker[AsyncSession],
    delivery_attempt: int,
) -> None:
    payment = await add_payment(session_factory, status=PaymentStatus.SUCCEEDED)
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)
    webhook_sender = FakeWebhookSender()
    processor = build_processor(session_factory, gateway, webhook_sender)

    with pytest.raises(ValueError, match="Invalid processing attempt"):
        await processor.process(
            payment_event(payment.id),
            delivery_attempt=delivery_attempt,
        )

    assert gateway.calls == []
    assert webhook_sender.calls == []
