from __future__ import annotations

import hashlib
import uuid
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from payment_service.config import Settings
from payment_service.domain import PaymentStatus
from payment_service.gateway import SimulatedPaymentGateway


async def test_gateway_is_deterministic_for_payment_id(settings: Settings) -> None:
    payment_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
    sleep = AsyncMock()
    configured_settings = settings.model_copy(
        update={
            "gateway_min_delay_seconds": 2.0,
            "gateway_max_delay_seconds": 5.0,
            "gateway_success_probability": 0.5,
        }
    )
    gateway = SimulatedPaymentGateway(configured_settings, sleep=sleep)

    first_outcome = await gateway.process(payment_id)
    second_outcome = await gateway.process(payment_id)

    digest = hashlib.sha256(payment_id.bytes).digest()
    expected_delay_fraction = int.from_bytes(digest[:8], byteorder="big") / ((1 << 64) - 1)
    expected_delay = 2.0 + (3.0 * expected_delay_fraction)
    expected_outcome_fraction = int.from_bytes(digest[8:16], byteorder="big") / (
        (1 << 64) - 1
    )
    expected_outcome = (
        PaymentStatus.SUCCEEDED if expected_outcome_fraction < 0.5 else PaymentStatus.FAILED
    )

    assert first_outcome is expected_outcome
    assert second_outcome is expected_outcome
    assert sleep.await_count == 2
    assert sleep.await_args_list[0].args[0] == pytest.approx(expected_delay)
    assert sleep.await_args_list[1].args[0] == pytest.approx(expected_delay)


@pytest.mark.parametrize(
    ("success_probability", "expected_status"),
    [
        (1.0, PaymentStatus.SUCCEEDED),
        (0.0, PaymentStatus.FAILED),
    ],
)
async def test_gateway_probability_boundaries(
    settings: Settings,
    success_probability: float,
    expected_status: PaymentStatus,
) -> None:
    sleep = AsyncMock()
    gateway = SimulatedPaymentGateway(
        settings.model_copy(update={"gateway_success_probability": success_probability}),
        sleep=sleep,
    )

    outcome = await gateway.process(uuid.uuid4())

    assert outcome is expected_status
    sleep.assert_awaited_once_with(0.0)


def test_settings_reject_inverted_gateway_delay_range() -> None:
    with pytest.raises(
        ValidationError,
        match="gateway_max_delay_seconds must be >= gateway_min_delay_seconds",
    ):
        Settings(
            _env_file=None,
            gateway_min_delay_seconds=2,
            gateway_max_delay_seconds=1,
        )


@pytest.mark.parametrize("invalid_value", [float("nan"), float("inf"), float("-inf")])
def test_settings_reject_non_finite_numbers(invalid_value: float) -> None:
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            webhook_timeout_seconds=invalid_value,
        )
