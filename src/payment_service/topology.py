from __future__ import annotations

from dataclasses import dataclass

from faststream.rabbit import (
    Channel,
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)

from payment_service.config import Settings
from payment_service.domain import (
    PAYMENTS_DEAD_LETTER_EXCHANGE_NAME,
    PAYMENTS_DEAD_LETTER_QUEUE_NAME,
    PAYMENTS_DEAD_LETTER_ROUTING_KEY,
    PAYMENTS_EXCHANGE_NAME,
    PAYMENTS_NEW_QUEUE_NAME,
    PAYMENTS_NEW_ROUTING_KEY,
    PAYMENTS_RETRY_EXCHANGE_NAME,
)


@dataclass(frozen=True, slots=True)
class RabbitTopology:
    payments_exchange: RabbitExchange
    retry_exchange: RabbitExchange
    dead_letter_exchange: RabbitExchange
    payments_queue: RabbitQueue
    retry_attempt_2_queue: RabbitQueue
    retry_attempt_3_queue: RabbitQueue
    dead_letter_queue: RabbitQueue

    def retry_queue_for_attempt(self, attempt: int) -> RabbitQueue:
        if attempt == 2:
            return self.retry_attempt_2_queue
        if attempt == 3:
            return self.retry_attempt_3_queue
        raise ValueError(f"No retry queue exists for attempt {attempt}")


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        fail_fast=True,
        reconnect_interval=settings.broker_reconnect_interval_seconds,
        default_channel=Channel(
            prefetch_count=1,
            publisher_confirms=True,
            on_return_raises=True,
        ),
    )


def build_topology(settings: Settings) -> RabbitTopology:
    payments_exchange = RabbitExchange(
        PAYMENTS_EXCHANGE_NAME,
        ExchangeType.DIRECT,
        durable=True,
    )
    retry_exchange = RabbitExchange(
        PAYMENTS_RETRY_EXCHANGE_NAME,
        ExchangeType.DIRECT,
        durable=True,
    )
    dead_letter_exchange = RabbitExchange(
        PAYMENTS_DEAD_LETTER_EXCHANGE_NAME,
        ExchangeType.DIRECT,
        durable=True,
    )
    payments_queue = RabbitQueue(
        PAYMENTS_NEW_QUEUE_NAME,
        durable=True,
        routing_key=PAYMENTS_NEW_ROUTING_KEY,
        arguments={
            "x-dead-letter-exchange": dead_letter_exchange.name,
            "x-dead-letter-routing-key": PAYMENTS_DEAD_LETTER_ROUTING_KEY,
            "x-single-active-consumer": True,
        },
    )

    base_retry_delay_ms = max(1, round(settings.retry_base_delay_seconds * 1000))
    retry_attempt_2_queue = RabbitQueue(
        "payments.retry.2",
        durable=True,
        routing_key="payments.retry.2",
        arguments={
            "x-message-ttl": base_retry_delay_ms,
            "x-dead-letter-exchange": payments_exchange.name,
            "x-dead-letter-routing-key": payments_queue.routing(),
        },
    )
    retry_attempt_3_queue = RabbitQueue(
        "payments.retry.3",
        durable=True,
        routing_key="payments.retry.3",
        arguments={
            "x-message-ttl": base_retry_delay_ms * 2,
            "x-dead-letter-exchange": payments_exchange.name,
            "x-dead-letter-routing-key": payments_queue.routing(),
        },
    )
    dead_letter_queue = RabbitQueue(
        PAYMENTS_DEAD_LETTER_QUEUE_NAME,
        durable=True,
        routing_key=PAYMENTS_DEAD_LETTER_ROUTING_KEY,
    )
    return RabbitTopology(
        payments_exchange=payments_exchange,
        retry_exchange=retry_exchange,
        dead_letter_exchange=dead_letter_exchange,
        payments_queue=payments_queue,
        retry_attempt_2_queue=retry_attempt_2_queue,
        retry_attempt_3_queue=retry_attempt_3_queue,
        dead_letter_queue=dead_letter_queue,
    )


async def declare_topology(broker: RabbitBroker, topology: RabbitTopology) -> None:
    exchanges = (
        topology.payments_exchange,
        topology.retry_exchange,
        topology.dead_letter_exchange,
    )
    declared_exchanges = {
        exchange.name: await broker.declare_exchange(exchange) for exchange in exchanges
    }

    queue_bindings = (
        (topology.payments_queue, topology.payments_exchange),
        (topology.retry_attempt_2_queue, topology.retry_exchange),
        (topology.retry_attempt_3_queue, topology.retry_exchange),
        (topology.dead_letter_queue, topology.dead_letter_exchange),
    )
    for queue_schema, exchange_schema in queue_bindings:
        declared_queue = await broker.declare_queue(queue_schema)
        await declared_queue.bind(
            exchange=declared_exchanges[exchange_schema.name],
            routing_key=queue_schema.routing(),
            arguments=queue_schema.bind_arguments,
            timeout=queue_schema.timeout,
            robust=queue_schema.robust,
        )
