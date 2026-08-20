from __future__ import annotations

import asyncio
import logging
import math
import uuid
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from faststream.rabbit import RabbitBroker
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import Settings
from payment_service.domain import RETRY_ATTEMPT_HEADER
from payment_service.models import OutboxEvent
from payment_service.topology import RabbitTopology, declare_topology

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ClaimedOutboxEvent:
    id: uuid.UUID
    routing_key: str
    payload: dict[str, Any]


def calculate_backoff_seconds(
    *,
    attempt: int,
    base_delay_seconds: float,
    max_backoff_seconds: float,
) -> float:
    if attempt < 1:
        raise ValueError("Attempt must be positive")
    if base_delay_seconds >= max_backoff_seconds:
        return max_backoff_seconds

    exponent = attempt - 1
    saturation_exponent = math.ceil(
        math.log2(max_backoff_seconds) - math.log2(base_delay_seconds)
    )
    if exponent >= saturation_exponent:
        return max_backoff_seconds
    return min(
        max_backoff_seconds,
        math.ldexp(base_delay_seconds, exponent),
    )


async def claim_outbox_events(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    lease_id: uuid.UUID,
    now: datetime,
    batch_size: int,
    lease_seconds: float,
) -> list[ClaimedOutboxEvent]:
    async with session_factory.begin() as session:
        statement = (
            select(OutboxEvent)
            .where(
                OutboxEvent.published_at.is_(None),
                OutboxEvent.next_attempt_at <= now,
                or_(
                    OutboxEvent.leased_until.is_(None),
                    OutboxEvent.leased_until <= now,
                ),
            )
            .order_by(OutboxEvent.created_at, OutboxEvent.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True, of=OutboxEvent)
        )
        events = (await session.execute(statement)).scalars().all()
        leased_until = now + timedelta(seconds=lease_seconds)
        claimed_events = []
        for event in events:
            event.lease_id = lease_id
            event.leased_until = leased_until
            claimed_events.append(
                ClaimedOutboxEvent(
                    id=event.id,
                    routing_key=event.routing_key,
                    payload=dict(event.payload),
                )
            )
        return claimed_events


async def mark_outbox_event_published(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    event_id: uuid.UUID,
    lease_id: uuid.UUID,
    published_at: datetime,
) -> None:
    async with session_factory.begin() as session:
        event = await session.scalar(
            select(OutboxEvent)
            .where(
                OutboxEvent.id == event_id,
                OutboxEvent.lease_id == lease_id,
                OutboxEvent.published_at.is_(None),
            )
            .with_for_update()
        )
        if event is None:
            return
        event.published_at = published_at
        event.lease_id = None
        event.leased_until = None
        event.last_error = None


async def record_outbox_publish_failure(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    event_id: uuid.UUID,
    lease_id: uuid.UUID,
    failed_at: datetime,
    error: BaseException,
    base_delay_seconds: float,
    max_backoff_seconds: float,
) -> None:
    async with session_factory.begin() as session:
        event = await session.scalar(
            select(OutboxEvent)
            .where(
                OutboxEvent.id == event_id,
                OutboxEvent.lease_id == lease_id,
                OutboxEvent.published_at.is_(None),
            )
            .with_for_update()
        )
        if event is None:
            return
        event.publish_attempts += 1
        backoff_seconds = calculate_backoff_seconds(
            attempt=event.publish_attempts,
            base_delay_seconds=base_delay_seconds,
            max_backoff_seconds=max_backoff_seconds,
        )
        event.next_attempt_at = failed_at + timedelta(seconds=backoff_seconds)
        event.last_error = type(error).__name__
        event.lease_id = None
        event.leased_until = None


async def release_outbox_claims(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    event_ids: Sequence[uuid.UUID],
    lease_id: uuid.UUID,
) -> None:
    if not event_ids:
        return
    async with session_factory.begin() as session:
        events = (
            (
                await session.execute(
                    select(OutboxEvent)
                    .where(
                        OutboxEvent.id.in_(event_ids),
                        OutboxEvent.lease_id == lease_id,
                        OutboxEvent.published_at.is_(None),
                    )
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        for event in events:
            event.lease_id = None
            event.leased_until = None


class OutboxRelay:
    def __init__(
        self,
        *,
        broker: RabbitBroker,
        topology: RabbitTopology,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
    ) -> None:
        self._broker = broker
        self._topology = topology
        self._session_factory = session_factory
        self._settings = settings
        self._stop_event = asyncio.Event()
        self._broker_started = False

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        try:
            while not self._stop_event.is_set():
                if not await self._ensure_broker_started():
                    await self._wait(self._settings.broker_reconnect_interval_seconds)
                    continue
                try:
                    await self._publish_next_batch()
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # noqa: BLE001
                    logger.warning(
                        "Outbox relay iteration failed",
                        extra={"error_type": type(error).__name__},
                    )
                    await self._wait(self._settings.outbox_poll_interval_seconds)
        finally:
            if self._broker_started:
                with suppress(Exception):
                    await self._broker.stop()
                self._broker_started = False

    async def _ensure_broker_started(self) -> bool:
        if self._broker_started:
            return True
        try:
            await self._broker.start()
            await declare_topology(self._broker, self._topology)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            with suppress(Exception):
                await self._broker.stop()
            logger.warning(
                "Outbox broker connection failed",
                extra={"error_type": type(error).__name__},
            )
            return False
        self._broker_started = True
        return True

    async def _publish_next_batch(self) -> None:
        lease_id = uuid.uuid4()
        try:
            events = await claim_outbox_events(
                self._session_factory,
                lease_id=lease_id,
                now=datetime.now(UTC),
                batch_size=self._settings.outbox_batch_size,
                lease_seconds=self._settings.outbox_lease_seconds,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            logger.warning(
                "Outbox database poll failed",
                extra={"error_type": type(error).__name__},
            )
            await self._wait(self._settings.outbox_poll_interval_seconds)
            return

        if not events:
            await self._wait(self._settings.outbox_poll_interval_seconds)
            return

        for index, event in enumerate(events):
            try:
                await self._broker.publish(
                    event.payload,
                    exchange=self._topology.payments_exchange,
                    routing_key=event.routing_key,
                    mandatory=True,
                    timeout=self._settings.broker_publish_timeout_seconds,
                    persist=True,
                    headers={RETRY_ATTEMPT_HEADER: 1},
                    correlation_id=str(event.id),
                    message_id=str(event.id),
                )
                await mark_outbox_event_published(
                    self._session_factory,
                    event_id=event.id,
                    lease_id=lease_id,
                    published_at=datetime.now(UTC),
                )
            except asyncio.CancelledError:
                await release_outbox_claims(
                    self._session_factory,
                    event_ids=[remaining.id for remaining in events[index:]],
                    lease_id=lease_id,
                )
                raise
            except Exception as error:  # noqa: BLE001
                await record_outbox_publish_failure(
                    self._session_factory,
                    event_id=event.id,
                    lease_id=lease_id,
                    failed_at=datetime.now(UTC),
                    error=error,
                    base_delay_seconds=self._settings.retry_base_delay_seconds,
                    max_backoff_seconds=self._settings.outbox_max_backoff_seconds,
                )
                await release_outbox_claims(
                    self._session_factory,
                    event_ids=[remaining.id for remaining in events[index + 1 :]],
                    lease_id=lease_id,
                )
                logger.warning(
                    "Outbox event publication failed",
                    extra={
                        "event_id": str(event.id),
                        "error_type": type(error).__name__,
                    },
                )
                await self._restart_broker()
                return

    async def _restart_broker(self) -> None:
        if self._broker_started:
            with suppress(Exception):
                await self._broker.stop()
            self._broker_started = False

    async def _wait(self, delay_seconds: float) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay_seconds)
