from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from typing import Any

from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitMessage
from pydantic import ValidationError

from payment_service.config import get_settings
from payment_service.database import dispose_engine, get_session_factory
from payment_service.domain import MAX_PROCESSING_ATTEMPTS, RETRY_ATTEMPT_HEADER
from payment_service.gateway import SimulatedPaymentGateway
from payment_service.processor import PaymentProcessor, RetryableProcessingError
from payment_service.schemas import PaymentCreatedEvent
from payment_service.topology import build_topology, create_broker, declare_topology
from payment_service.webhooks import HttpWebhookSender

logger = logging.getLogger(__name__)

settings = get_settings()
topology = build_topology(settings)
broker = create_broker(settings)
app = FastStream(broker)
webhook_sender = HttpWebhookSender(settings)
processor = PaymentProcessor(
    session_factory=get_session_factory(),
    gateway=SimulatedPaymentGateway(settings),
    webhook_sender=webhook_sender,
    webhook_timeout_seconds=settings.webhook_timeout_seconds,
)


def parse_delivery_attempt(headers: Mapping[str, Any]) -> int:
    raw_attempt = headers.get(RETRY_ATTEMPT_HEADER, 1)
    if isinstance(raw_attempt, bool):
        raise ValueError("Invalid processing attempt header")
    try:
        attempt = int(raw_attempt)
    except (TypeError, ValueError) as error:
        raise ValueError("Invalid processing attempt header") from error
    if not 1 <= attempt <= MAX_PROCESSING_ATTEMPTS:
        raise ValueError("Invalid processing attempt header")
    return attempt


def decode_raw_body(message: RabbitMessage) -> bytes:
    return message.body


@app.on_startup
async def setup_topology() -> None:
    await broker.connect()
    await declare_topology(broker, topology)


@app.after_shutdown
async def close_resources() -> None:
    await webhook_sender.close()
    await dispose_engine()


@broker.subscriber(
    topology.payments_queue,
    topology.payments_exchange,
    ack_policy=AckPolicy.MANUAL,
    decoder=decode_raw_body,
)
async def consume_payment(
    event_payload: bytes,
    message: RabbitMessage,
) -> None:
    try:
        event = PaymentCreatedEvent.model_validate_json(event_payload)
        delivery_attempt = parse_delivery_attempt(message.headers)
    except (ValidationError, ValueError):
        await message.reject(requeue=False)
        return

    try:
        await processor.process(event, delivery_attempt=delivery_attempt)
    except RetryableProcessingError as error:
        await retry_or_dead_letter(
            event,
            message,
            failed_attempt=max(delivery_attempt, error.attempt),
        )
    except asyncio.CancelledError:
        raise
    except Exception as error:  # noqa: BLE001
        logger.warning(
            "Payment message processing failed",
            extra={
                "event_id": str(event.event_id),
                "payment_id": str(event.payment_id),
                "error_type": type(error).__name__,
            },
        )
        await retry_or_dead_letter(
            event,
            message,
            failed_attempt=delivery_attempt,
        )
    else:
        await message.ack()


async def retry_or_dead_letter(
    event: PaymentCreatedEvent,
    message: RabbitMessage,
    *,
    failed_attempt: int,
) -> None:
    if failed_attempt >= MAX_PROCESSING_ATTEMPTS:
        await message.reject(requeue=False)
        return

    next_attempt = failed_attempt + 1
    retry_queue = topology.retry_queue_for_attempt(next_attempt)
    try:
        await broker.publish(
            event.model_dump(mode="json"),
            exchange=topology.retry_exchange,
            routing_key=retry_queue.routing(),
            mandatory=True,
            timeout=settings.broker_publish_timeout_seconds,
            persist=True,
            headers={RETRY_ATTEMPT_HEADER: next_attempt},
            correlation_id=str(event.event_id),
            message_id=str(event.event_id),
        )
    except asyncio.CancelledError:
        raise
    except Exception as error:  # noqa: BLE001
        logger.warning(
            "Retry publication failed",
            extra={
                "event_id": str(event.event_id),
                "next_attempt": next_attempt,
                "error_type": type(error).__name__,
            },
        )
        await message.nack(requeue=True)
        return
    await message.ack()
