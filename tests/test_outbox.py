from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service import outbox as outbox_module
from payment_service.config import Settings
from payment_service.domain import PAYMENT_CREATED_EVENT, PAYMENTS_NEW_ROUTING_KEY
from payment_service.models import OutboxEvent, Payment
from payment_service.outbox import (
    OutboxRelay,
    calculate_backoff_seconds,
    claim_outbox_events,
    mark_outbox_event_published,
    record_outbox_publish_failure,
    release_outbox_claims,
)
from payment_service.topology import build_topology

NOW = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)


async def add_outbox_event(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    event_id: uuid.UUID,
    created_at: datetime = NOW,
    next_attempt_at: datetime = NOW,
    published_at: datetime | None = None,
    publish_attempts: int = 0,
    lease_id: uuid.UUID | None = None,
    leased_until: datetime | None = None,
) -> OutboxEvent:
    payment_id = uuid.uuid5(uuid.NAMESPACE_URL, f"payment:{event_id}")
    payment = Payment(
        id=payment_id,
        amount="10.00",
        currency="RUB",
        description="Outbox test payment",
        metadata_={},
        status="pending",
        idempotency_key=str(payment_id),
        request_fingerprint="f" * 64,
        webhook_url="https://hooks.example.test/payments",
        created_at=created_at,
    )
    outbox_event = OutboxEvent(
        id=event_id,
        aggregate_id=payment_id,
        event_type=PAYMENT_CREATED_EVENT,
        routing_key=PAYMENTS_NEW_ROUTING_KEY,
        payload={"event_id": str(event_id), "payment_id": str(payment_id)},
        created_at=created_at,
        next_attempt_at=next_attempt_at,
        published_at=published_at,
        publish_attempts=publish_attempts,
        lease_id=lease_id,
        leased_until=leased_until,
    )
    async with session_factory.begin() as session:
        session.add_all((payment, outbox_event))
    return outbox_event


def normalized_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


@pytest.mark.parametrize(
    ("attempt", "expected_delay"),
    [
        (1, 2.0),
        (2, 4.0),
        (3, 5.0),
        (10_000, 5.0),
    ],
)
def test_calculate_backoff_seconds_saturates_without_overflow(
    attempt: int,
    expected_delay: float,
) -> None:
    assert calculate_backoff_seconds(
        attempt=attempt,
        base_delay_seconds=2.0,
        max_backoff_seconds=5.0,
    ) == expected_delay


def test_calculate_backoff_seconds_rejects_non_positive_attempt() -> None:
    with pytest.raises(ValueError, match="Attempt must be positive"):
        calculate_backoff_seconds(
            attempt=0,
            base_delay_seconds=2.0,
            max_backoff_seconds=5.0,
        )


def test_calculate_backoff_seconds_caps_initial_delay() -> None:
    assert calculate_backoff_seconds(
        attempt=1,
        base_delay_seconds=10.0,
        max_backoff_seconds=5.0,
    ) == 5.0


async def test_claim_outbox_events_filters_orders_and_leases(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    eligible_early = uuid.UUID(int=1)
    eligible_late = uuid.UUID(int=2)
    await add_outbox_event(
        session_factory,
        event_id=eligible_late,
        created_at=NOW - timedelta(seconds=5),
    )
    await add_outbox_event(
        session_factory,
        event_id=eligible_early,
        created_at=NOW - timedelta(seconds=10),
        lease_id=uuid.uuid4(),
        leased_until=NOW - timedelta(seconds=1),
    )
    await add_outbox_event(
        session_factory,
        event_id=uuid.UUID(int=3),
        next_attempt_at=NOW + timedelta(seconds=1),
    )
    await add_outbox_event(
        session_factory,
        event_id=uuid.UUID(int=4),
        lease_id=uuid.uuid4(),
        leased_until=NOW + timedelta(seconds=1),
    )
    await add_outbox_event(
        session_factory,
        event_id=uuid.UUID(int=5),
        published_at=NOW - timedelta(seconds=1),
    )
    lease_id = uuid.uuid4()

    claimed = await claim_outbox_events(
        session_factory,
        lease_id=lease_id,
        now=NOW,
        batch_size=10,
        lease_seconds=30,
    )

    assert [event.id for event in claimed] == [eligible_early, eligible_late]
    async with session_factory() as session:
        persisted = (
            (
                await session.execute(
                    select(OutboxEvent).where(OutboxEvent.id.in_([eligible_early, eligible_late]))
                )
            )
            .scalars()
            .all()
        )
    assert {event.lease_id for event in persisted} == {lease_id}
    assert {normalized_utc(event.leased_until) for event in persisted if event.leased_until} == {
        NOW + timedelta(seconds=30)
    }


async def test_mark_outbox_event_published_requires_matching_lease(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = uuid.uuid4()
    lease_id = uuid.uuid4()
    await add_outbox_event(
        session_factory,
        event_id=event_id,
        lease_id=lease_id,
        leased_until=NOW + timedelta(seconds=30),
    )

    await mark_outbox_event_published(
        session_factory,
        event_id=event_id,
        lease_id=uuid.uuid4(),
        published_at=NOW,
    )
    async with session_factory() as session:
        unchanged = await session.get(OutboxEvent, event_id)
    assert unchanged is not None
    assert unchanged.published_at is None

    await mark_outbox_event_published(
        session_factory,
        event_id=event_id,
        lease_id=lease_id,
        published_at=NOW,
    )
    async with session_factory() as session:
        published = await session.get(OutboxEvent, event_id)
    assert published is not None
    assert normalized_utc(published.published_at) == NOW if published.published_at else False
    assert published.lease_id is None
    assert published.leased_until is None
    assert published.last_error is None


async def test_record_publish_failure_applies_capped_backoff_and_releases_lease(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = uuid.uuid4()
    lease_id = uuid.uuid4()
    await add_outbox_event(
        session_factory,
        event_id=event_id,
        publish_attempts=2,
        lease_id=lease_id,
        leased_until=NOW + timedelta(seconds=30),
    )

    await record_outbox_publish_failure(
        session_factory,
        event_id=event_id,
        lease_id=lease_id,
        failed_at=NOW,
        error=ConnectionError("broker unavailable"),
        base_delay_seconds=2,
        max_backoff_seconds=5,
    )

    async with session_factory() as session:
        failed = await session.get(OutboxEvent, event_id)
    assert failed is not None
    assert failed.publish_attempts == 3
    assert normalized_utc(failed.next_attempt_at) == NOW + timedelta(seconds=5)
    assert failed.last_error == "ConnectionError"
    assert failed.lease_id is None
    assert failed.leased_until is None


async def test_record_publish_failure_handles_long_outage_without_overflow(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = uuid.uuid4()
    lease_id = uuid.uuid4()
    await add_outbox_event(
        session_factory,
        event_id=event_id,
        publish_attempts=9_999,
        lease_id=lease_id,
        leased_until=NOW + timedelta(seconds=30),
    )

    await record_outbox_publish_failure(
        session_factory,
        event_id=event_id,
        lease_id=lease_id,
        failed_at=NOW,
        error=ConnectionError("broker unavailable"),
        base_delay_seconds=2,
        max_backoff_seconds=5,
    )

    async with session_factory() as session:
        failed = await session.get(OutboxEvent, event_id)
    assert failed is not None
    assert failed.publish_attempts == 10_000
    assert normalized_utc(failed.next_attempt_at) == NOW + timedelta(seconds=5)
    assert failed.lease_id is None
    assert failed.leased_until is None


async def test_release_outbox_claims_only_releases_matching_claims(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    matching_event_id = uuid.uuid4()
    other_event_id = uuid.uuid4()
    lease_id = uuid.uuid4()
    other_lease_id = uuid.uuid4()
    await add_outbox_event(
        session_factory,
        event_id=matching_event_id,
        lease_id=lease_id,
        leased_until=NOW + timedelta(seconds=30),
    )
    await add_outbox_event(
        session_factory,
        event_id=other_event_id,
        lease_id=other_lease_id,
        leased_until=NOW + timedelta(seconds=30),
    )

    await release_outbox_claims(
        session_factory,
        event_ids=[matching_event_id, other_event_id],
        lease_id=lease_id,
    )

    async with session_factory() as session:
        matching_event = await session.get(OutboxEvent, matching_event_id)
        other_event = await session.get(OutboxEvent, other_event_id)
    assert matching_event is not None
    assert matching_event.lease_id is None
    assert matching_event.leased_until is None
    assert other_event is not None
    assert other_event.lease_id == other_lease_id


async def test_release_outbox_claims_accepts_empty_batch(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await release_outbox_claims(
        session_factory,
        event_ids=[],
        lease_id=uuid.uuid4(),
    )


async def test_outbox_relay_starts_broker_and_declares_topology_once(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    broker = SimpleNamespace(
        publish=AsyncMock(),
        start=AsyncMock(),
        stop=AsyncMock(),
    )
    topology = build_topology(settings)
    declare_topology = AsyncMock()
    monkeypatch.setattr(outbox_module, "declare_topology", declare_topology)
    relay = OutboxRelay(
        broker=cast(Any, broker),
        topology=topology,
        session_factory=session_factory,
        settings=settings,
    )

    assert await relay._ensure_broker_started() is True
    assert await relay._ensure_broker_started() is True

    broker.start.assert_awaited_once_with()
    declare_topology.assert_awaited_once_with(broker, topology)


async def test_outbox_relay_cleans_up_failed_broker_start(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    broker = SimpleNamespace(
        publish=AsyncMock(),
        start=AsyncMock(side_effect=ConnectionError("broker unavailable")),
        stop=AsyncMock(),
    )
    declare_topology = AsyncMock()
    monkeypatch.setattr(outbox_module, "declare_topology", declare_topology)
    relay = OutboxRelay(
        broker=cast(Any, broker),
        topology=build_topology(settings),
        session_factory=session_factory,
        settings=settings,
    )

    assert await relay._ensure_broker_started() is False

    broker.stop.assert_awaited_once_with()
    declare_topology.assert_not_awaited()
    assert relay._broker_started is False


async def test_outbox_relay_waits_when_no_events_are_available(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    broker = SimpleNamespace(
        publish=AsyncMock(),
        start=AsyncMock(),
        stop=AsyncMock(),
    )
    relay = OutboxRelay(
        broker=cast(Any, broker),
        topology=build_topology(settings),
        session_factory=session_factory,
        settings=settings,
    )
    wait = AsyncMock()
    cast(Any, relay)._wait = wait

    await relay._publish_next_batch()

    wait.assert_awaited_once_with(settings.outbox_poll_interval_seconds)
    broker.publish.assert_not_awaited()


async def test_outbox_relay_releases_entire_batch_when_cancelled(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    event_ids = [uuid.UUID(int=101), uuid.UUID(int=102)]
    for index, event_id in enumerate(event_ids):
        await add_outbox_event(
            session_factory,
            event_id=event_id,
            created_at=datetime.now(UTC) - timedelta(seconds=2 - index),
            next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    broker = SimpleNamespace(
        publish=AsyncMock(side_effect=asyncio.CancelledError),
        start=AsyncMock(),
        stop=AsyncMock(),
    )
    relay = OutboxRelay(
        broker=cast(Any, broker),
        topology=build_topology(settings),
        session_factory=session_factory,
        settings=settings,
    )

    with pytest.raises(asyncio.CancelledError):
        await relay._publish_next_batch()

    async with session_factory() as session:
        events = (
            (await session.execute(select(OutboxEvent).where(OutboxEvent.id.in_(event_ids))))
            .scalars()
            .all()
        )
    assert {event.lease_id for event in events} == {None}
    assert {event.leased_until for event in events} == {None}


async def test_outbox_relay_publishes_and_marks_event(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    event_id = uuid.uuid4()
    await add_outbox_event(
        session_factory,
        event_id=event_id,
        next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    broker = SimpleNamespace(
        publish=AsyncMock(),
        start=AsyncMock(),
        stop=AsyncMock(),
    )
    topology = build_topology(settings)
    relay = OutboxRelay(
        broker=cast(Any, broker),
        topology=topology,
        session_factory=session_factory,
        settings=settings,
    )

    await relay._publish_next_batch()

    broker.publish.assert_awaited_once()
    publish_arguments = broker.publish.await_args.kwargs
    assert publish_arguments["routing_key"] == PAYMENTS_NEW_ROUTING_KEY
    assert publish_arguments["mandatory"] is True
    assert publish_arguments["persist"] is True
    assert publish_arguments["correlation_id"] == str(event_id)
    async with session_factory() as session:
        published = await session.get(OutboxEvent, event_id)
    assert published is not None
    assert published.published_at is not None


async def test_outbox_relay_records_failure_and_restarts_broker(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    event_id = uuid.uuid4()
    await add_outbox_event(
        session_factory,
        event_id=event_id,
        next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    broker = SimpleNamespace(
        publish=AsyncMock(side_effect=ConnectionError("broker unavailable")),
        start=AsyncMock(),
        stop=AsyncMock(),
    )
    relay = OutboxRelay(
        broker=cast(Any, broker),
        topology=build_topology(settings),
        session_factory=session_factory,
        settings=settings,
    )
    relay._broker_started = True

    await relay._publish_next_batch()

    broker.stop.assert_awaited_once()
    assert relay._broker_started is False
    async with session_factory() as session:
        failed = await session.get(OutboxEvent, event_id)
    assert failed is not None
    assert failed.published_at is None
    assert failed.publish_attempts == 1
    assert failed.last_error == "ConnectionError"
    assert failed.lease_id is None
