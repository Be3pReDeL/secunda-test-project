from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.domain import (
    PAYMENT_CREATED_EVENT,
    PAYMENTS_NEW_ROUTING_KEY,
    PaymentStatus,
)
from payment_service.errors import (
    IdempotencyConflictError,
    InvalidIdempotencyKeyError,
    PaymentNotFoundError,
)
from payment_service.models import OutboxEvent, Payment
from payment_service.schemas import PaymentCreate, PaymentCreatedEvent

MAX_IDEMPOTENCY_KEY_LENGTH = 255


def validate_idempotency_key(idempotency_key: str) -> None:
    if (
        not idempotency_key
        or len(idempotency_key) > MAX_IDEMPOTENCY_KEY_LENGTH
        or not idempotency_key.isprintable()
        or not idempotency_key.strip()
    ):
        raise InvalidIdempotencyKeyError


def ensure_matching_fingerprint(payment: Payment, request_fingerprint: str) -> None:
    if payment.request_fingerprint != request_fingerprint:
        raise IdempotencyConflictError


async def create_payment(
    session: AsyncSession,
    payment_data: PaymentCreate,
    idempotency_key: str,
) -> Payment:
    validate_idempotency_key(idempotency_key)
    request_fingerprint = payment_data.fingerprint()

    async with session.begin():
        existing_payment = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if existing_payment is not None:
            ensure_matching_fingerprint(existing_payment, request_fingerprint)
            return existing_payment

        payment_id = uuid.uuid4()
        event_id = uuid.uuid4()
        created_at = datetime.now(UTC)
        payment = Payment(
            id=payment_id,
            amount=payment_data.amount,
            currency=payment_data.currency.value,
            description=payment_data.description,
            metadata_=payment_data.metadata,
            status=PaymentStatus.PENDING.value,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            webhook_url=str(payment_data.webhook_url),
            webhook_attempts=0,
            created_at=created_at,
        )
        event = PaymentCreatedEvent(
            event_id=event_id,
            payment_id=payment_id,
            event_type=PAYMENT_CREATED_EVENT,
            occurred_at=created_at,
        )
        outbox_event = OutboxEvent(
            id=event_id,
            aggregate_id=payment_id,
            event_type=PAYMENT_CREATED_EVENT,
            routing_key=PAYMENTS_NEW_ROUTING_KEY,
            payload=event.model_dump(mode="json"),
            created_at=created_at,
            next_attempt_at=created_at,
        )

        try:
            async with session.begin_nested():
                session.add(payment)
                await session.flush((payment,))
                session.add(outbox_event)
                await session.flush((outbox_event,))
        except IntegrityError:
            concurrent_payment = await session.scalar(
                select(Payment).where(Payment.idempotency_key == idempotency_key)
            )
            if concurrent_payment is None:
                raise
            ensure_matching_fingerprint(concurrent_payment, request_fingerprint)
            return concurrent_payment

        return payment


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise PaymentNotFoundError
    return payment
