from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from faststream.rabbit import RabbitMessage, TestRabbitBroker

from payment_service import worker
from payment_service.config import Settings
from payment_service.database import dispose_engine
from payment_service.domain import (
    MAX_PROCESSING_ATTEMPTS,
    PAYMENT_CREATED_EVENT,
    RETRY_ATTEMPT_HEADER,
)
from payment_service.processor import RetryableProcessingError
from payment_service.schemas import PaymentCreatedEvent
from payment_service.topology import build_topology, declare_topology


class FakeMessage:
    def __init__(
        self,
        headers: dict[str, Any] | None = None,
        *,
        body: bytes = b"",
    ) -> None:
        self.headers = headers or {}
        self.body = body
        self.ack = AsyncMock()
        self.nack = AsyncMock()
        self.reject = AsyncMock()


class FakeDeclaredQueue:
    def __init__(self, name: str) -> None:
        self.name = name
        self.bindings: list[dict[str, Any]] = []

    async def bind(self, **arguments: Any) -> None:
        self.bindings.append(arguments)


class FakeDeclarationBroker:
    def __init__(self) -> None:
        self.exchange_names: list[str] = []
        self.queues: dict[str, FakeDeclaredQueue] = {}

    async def declare_exchange(self, exchange: Any) -> str:
        self.exchange_names.append(exchange.name)
        return f"declared:{exchange.name}"

    async def declare_queue(self, queue: Any) -> FakeDeclaredQueue:
        declared_queue = FakeDeclaredQueue(queue.name)
        self.queues[queue.name] = declared_queue
        return declared_queue


@pytest_asyncio.fixture(scope="module", autouse=True)
async def close_worker_resources() -> Any:
    yield
    await worker.webhook_sender.close()
    await dispose_engine()


def test_build_topology_configures_retry_and_dead_letter_routing(settings: Settings) -> None:
    topology = build_topology(settings.model_copy(update={"retry_base_delay_seconds": 1.25}))

    assert topology.payments_exchange.name == "payments"
    assert topology.retry_exchange.name == "payments.retry"
    assert topology.dead_letter_exchange.name == "payments.dlx"
    assert topology.payments_queue.routing() == "payments.new"
    assert topology.payments_queue.arguments["x-dead-letter-exchange"] == "payments.dlx"
    assert topology.payments_queue.arguments["x-dead-letter-routing-key"] == "payments.dead"
    assert topology.retry_attempt_2_queue.arguments == {
        "x-queue-type": "classic",
        "x-message-ttl": 1250,
        "x-dead-letter-exchange": "payments",
        "x-dead-letter-routing-key": "payments.new",
    }
    assert topology.retry_attempt_3_queue.arguments == {
        "x-queue-type": "classic",
        "x-message-ttl": 2500,
        "x-dead-letter-exchange": "payments",
        "x-dead-letter-routing-key": "payments.new",
    }
    assert topology.retry_queue_for_attempt(2) is topology.retry_attempt_2_queue
    assert topology.retry_queue_for_attempt(3) is topology.retry_attempt_3_queue
    with pytest.raises(ValueError, match="No retry queue"):
        topology.retry_queue_for_attempt(1)
    with pytest.raises(ValueError, match="No retry queue"):
        topology.retry_queue_for_attempt(4)


async def test_declare_topology_declares_and_binds_all_entities(settings: Settings) -> None:
    topology = build_topology(settings)
    broker = FakeDeclarationBroker()

    await declare_topology(cast(Any, broker), topology)

    assert broker.exchange_names == ["payments", "payments.retry", "payments.dlx"]
    assert set(broker.queues) == {
        "payments.new",
        "payments.retry.2",
        "payments.retry.3",
        "payments.dlq",
    }
    expected_exchanges = {
        "payments.new": "declared:payments",
        "payments.retry.2": "declared:payments.retry",
        "payments.retry.3": "declared:payments.retry",
        "payments.dlq": "declared:payments.dlx",
    }
    for queue_name, expected_exchange in expected_exchanges.items():
        assert broker.queues[queue_name].bindings[0]["exchange"] == expected_exchange


@pytest.mark.parametrize(
    ("headers", "expected_attempt"),
    [
        ({}, 1),
        ({RETRY_ATTEMPT_HEADER: 2}, 2),
        ({RETRY_ATTEMPT_HEADER: "3"}, 3),
    ],
)
def test_parse_delivery_attempt_accepts_supported_headers(
    headers: dict[str, Any],
    expected_attempt: int,
) -> None:
    assert worker.parse_delivery_attempt(headers) == expected_attempt


@pytest.mark.parametrize(
    "raw_attempt",
    [None, True, False, 0, -1, MAX_PROCESSING_ATTEMPTS + 1, "not-a-number"],
)
def test_parse_delivery_attempt_rejects_invalid_headers(raw_attempt: object) -> None:
    with pytest.raises(ValueError, match="Invalid processing attempt header"):
        worker.parse_delivery_attempt({RETRY_ATTEMPT_HEADER: raw_attempt})


def test_decode_raw_body_preserves_unparsed_bytes() -> None:
    raw_body = b"\xffnot-json\x00"

    assert worker.decode_raw_body(cast(Any, FakeMessage(body=raw_body))) is raw_body


def make_event() -> PaymentCreatedEvent:
    return PaymentCreatedEvent(
        event_id=uuid.uuid4(),
        payment_id=uuid.uuid4(),
        event_type=PAYMENT_CREATED_EVENT,
        occurred_at=datetime.now(UTC),
    )


async def test_retry_route_publishes_next_attempt_and_acks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = make_event()
    message = FakeMessage()
    broker = SimpleNamespace(publish=AsyncMock())
    monkeypatch.setattr(worker, "broker", broker)

    await worker.retry_or_dead_letter(event, cast(Any, message), failed_attempt=1)

    broker.publish.assert_awaited_once()
    publish_arguments = broker.publish.await_args.kwargs
    assert publish_arguments["exchange"] is worker.topology.retry_exchange
    assert publish_arguments["routing_key"] == "payments.retry.2"
    assert publish_arguments["headers"] == {RETRY_ATTEMPT_HEADER: 2}
    assert publish_arguments["message_id"] == str(event.event_id)
    message.ack.assert_awaited_once_with()
    message.nack.assert_not_awaited()
    message.reject.assert_not_awaited()


async def test_retry_route_dead_letters_exhausted_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = make_event()
    message = FakeMessage()
    broker = SimpleNamespace(publish=AsyncMock())
    monkeypatch.setattr(worker, "broker", broker)

    await worker.retry_or_dead_letter(
        event,
        cast(Any, message),
        failed_attempt=MAX_PROCESSING_ATTEMPTS,
    )

    broker.publish.assert_not_awaited()
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()


async def test_retry_route_requeues_original_when_publish_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = make_event()
    message = FakeMessage()
    broker = SimpleNamespace(
        publish=AsyncMock(side_effect=ConnectionError("broker unavailable")),
    )
    monkeypatch.setattr(worker, "broker", broker)

    await worker.retry_or_dead_letter(event, cast(Any, message), failed_attempt=1)

    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()
    message.reject.assert_not_awaited()


async def test_consumer_acks_successful_processing(monkeypatch: pytest.MonkeyPatch) -> None:
    event = make_event()
    message = FakeMessage({RETRY_ATTEMPT_HEADER: 1})
    processor = SimpleNamespace(process=AsyncMock())
    monkeypatch.setattr(worker, "processor", processor)

    await worker.consume_payment(event.model_dump_json().encode(), cast(Any, message))

    processor.process.assert_awaited_once_with(event, delivery_attempt=1)
    message.ack.assert_awaited_once_with()


async def test_consumer_routes_retryable_failure_with_highest_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = make_event()
    message = FakeMessage({RETRY_ATTEMPT_HEADER: 2})
    processor = SimpleNamespace(process=AsyncMock(side_effect=RetryableProcessingError(1)))
    retry = AsyncMock()
    monkeypatch.setattr(worker, "processor", processor)
    monkeypatch.setattr(worker, "retry_or_dead_letter", retry)

    await worker.consume_payment(event.model_dump_json().encode(), cast(Any, message))

    processor.process.assert_awaited_once_with(event, delivery_attempt=2)
    retry.assert_awaited_once_with(event, message, failed_attempt=2)
    message.ack.assert_not_awaited()


async def test_consumer_routes_unexpected_failure_using_delivery_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = make_event()
    message = FakeMessage({RETRY_ATTEMPT_HEADER: 2})
    processor = SimpleNamespace(process=AsyncMock(side_effect=RuntimeError("unexpected")))
    retry = AsyncMock()
    monkeypatch.setattr(worker, "processor", processor)
    monkeypatch.setattr(worker, "retry_or_dead_letter", retry)

    await worker.consume_payment(event.model_dump_json().encode(), cast(Any, message))

    processor.process.assert_awaited_once_with(event, delivery_attempt=2)
    retry.assert_awaited_once_with(event, message, failed_attempt=2)
    message.ack.assert_not_awaited()


async def test_consumer_rejects_invalid_payload_without_processing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message = FakeMessage()
    processor = SimpleNamespace(process=AsyncMock())
    monkeypatch.setattr(worker, "processor", processor)

    await worker.consume_payment(b'{"event_id":"invalid"}', cast(Any, message))

    processor.process.assert_not_awaited()
    message.reject.assert_awaited_once_with(requeue=False)


@pytest.mark.parametrize(
    "raw_payload",
    [
        pytest.param(b'{"event_id":', id="malformed-json"),
        pytest.param(b"[]", id="json-list"),
        pytest.param(b"42", id="json-number"),
        pytest.param(b'"scalar"', id="json-string"),
    ],
)
async def test_test_broker_rejects_non_event_json_without_hanging(
    monkeypatch: pytest.MonkeyPatch,
    raw_payload: bytes,
) -> None:
    processor = SimpleNamespace(process=AsyncMock())
    reject = AsyncMock()
    monkeypatch.setattr(worker, "processor", processor)
    monkeypatch.setattr(RabbitMessage, "reject", reject)

    async with TestRabbitBroker(worker.broker) as test_broker:
        await asyncio.wait_for(
            test_broker.publish(
                raw_payload,
                exchange=worker.topology.payments_exchange,
                routing_key=worker.topology.payments_queue.routing(),
                headers={RETRY_ATTEMPT_HEADER: 1},
                content_type="application/json",
            ),
            timeout=1,
        )

    processor.process.assert_not_awaited()
    reject.assert_awaited_once_with(requeue=False)
