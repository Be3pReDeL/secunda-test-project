from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.domain import MAX_PROCESSING_ATTEMPTS, PaymentStatus
from payment_service.errors import PaymentNotFoundError
from payment_service.models import Payment
from payment_service.schemas import PaymentCreatedEvent, WebhookPayload


class Gateway(Protocol):
    async def process(self, payment_id: uuid.UUID) -> PaymentStatus: ...


class WebhookSender(Protocol):
    async def send(
        self,
        webhook_url: str,
        payload: WebhookPayload,
        *,
        event_id: uuid.UUID,
    ) -> None: ...


class RetryableProcessingError(Exception):
    def __init__(self, attempt: int) -> None:
        super().__init__(f"Payment processing attempt {attempt} failed")
        self.attempt = attempt


class PaymentProcessor:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: Gateway,
        webhook_sender: WebhookSender,
        webhook_timeout_seconds: float,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhook_sender = webhook_sender
        self._webhook_timeout_seconds = webhook_timeout_seconds

    async def process(
        self,
        event: PaymentCreatedEvent,
        *,
        delivery_attempt: int,
    ) -> None:
        await self._ensure_payment_is_terminal(event.payment_id)
        await self._deliver_webhook(event, delivery_attempt=delivery_attempt)

    async def _ensure_payment_is_terminal(self, payment_id: uuid.UUID) -> None:
        async with self._session_factory() as session:
            payment = await session.get(Payment, payment_id)
            if payment is None:
                raise PaymentNotFoundError
            if payment.status != PaymentStatus.PENDING.value:
                return

        outcome = await self._gateway.process(payment_id)
        processed_at = datetime.now(UTC)
        async with self._session_factory.begin() as session:
            await session.execute(
                update(Payment)
                .where(
                    Payment.id == payment_id,
                    Payment.status == PaymentStatus.PENDING.value,
                )
                .values(
                    status=outcome.value,
                    processed_at=processed_at,
                )
            )

    async def _deliver_webhook(
        self,
        event: PaymentCreatedEvent,
        *,
        delivery_attempt: int,
    ) -> None:
        if not 1 <= delivery_attempt <= MAX_PROCESSING_ATTEMPTS:
            raise ValueError("Invalid processing attempt")

        delivery_error: Exception | None = None
        retry_error: RetryableProcessingError | None = None
        async with self._session_factory.begin() as session:
            payment = await session.scalar(
                select(Payment).where(Payment.id == event.payment_id).with_for_update()
            )
            if payment is None:
                raise PaymentNotFoundError
            if payment.webhook_delivered_at is not None:
                return
            if payment.status == PaymentStatus.PENDING.value or payment.processed_at is None:
                raise RuntimeError("Payment must be terminal before webhook delivery")
            if payment.webhook_attempts >= delivery_attempt:
                retry_error = RetryableProcessingError(payment.webhook_attempts)
            else:
                payload = WebhookPayload(
                    event_id=event.event_id,
                    payment_id=event.payment_id,
                    status=PaymentStatus(payment.status),
                    processed_at=payment.processed_at,
                )
                try:
                    async with asyncio.timeout(self._webhook_timeout_seconds):
                        await self._webhook_sender.send(
                            payment.webhook_url,
                            payload,
                            event_id=event.event_id,
                        )
                except Exception as error:  # noqa: BLE001
                    payment.webhook_attempts = delivery_attempt
                    payment.webhook_last_error = type(error).__name__
                    delivery_error = error
                    retry_error = RetryableProcessingError(delivery_attempt)
                else:
                    payment.webhook_attempts = delivery_attempt
                    payment.webhook_delivered_at = datetime.now(UTC)
                    payment.webhook_last_error = None

        if retry_error is not None:
            raise retry_error from delivery_error
