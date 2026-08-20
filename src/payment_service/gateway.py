from __future__ import annotations

import asyncio
import hashlib
import uuid
from collections.abc import Awaitable, Callable

from payment_service.config import Settings
from payment_service.domain import PaymentStatus


class SimulatedPaymentGateway:
    def __init__(
        self,
        settings: Settings,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._min_delay_seconds = settings.gateway_min_delay_seconds
        self._max_delay_seconds = settings.gateway_max_delay_seconds
        self._success_probability = settings.gateway_success_probability
        self._sleep = sleep

    async def process(self, payment_id: uuid.UUID) -> PaymentStatus:
        digest = hashlib.sha256(payment_id.bytes).digest()
        delay_fraction = int.from_bytes(digest[:8], byteorder="big") / ((1 << 64) - 1)
        delay_seconds = self._min_delay_seconds + (
            (self._max_delay_seconds - self._min_delay_seconds) * delay_fraction
        )
        await self._sleep(delay_seconds)

        outcome_fraction = int.from_bytes(digest[8:16], byteorder="big") / ((1 << 64) - 1)
        if outcome_fraction < self._success_probability:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
