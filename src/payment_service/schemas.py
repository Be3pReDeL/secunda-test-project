from __future__ import annotations

import hashlib
import json
import math
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, JsonValue, field_validator

from payment_service.domain import Currency, PaymentStatus

Amount = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]


def _validate_finite_json(value: JsonValue) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("metadata must contain only finite numbers")
    if isinstance(value, list):
        for item in value:
            _validate_finite_json(item)
    elif isinstance(value, dict):
        for item in value.values():
            _validate_finite_json(item)


class PaymentCreate(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    amount: Amount
    currency: Currency
    description: Annotated[str, Field(min_length=1, max_length=1000)]
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: AnyHttpUrl

    @field_validator("webhook_url")
    @classmethod
    def reject_webhook_credentials(cls, webhook_url: AnyHttpUrl) -> AnyHttpUrl:
        if webhook_url.username is not None or webhook_url.password is not None:
            raise ValueError("webhook_url must not contain credentials")
        return webhook_url

    @field_validator("metadata")
    @classmethod
    def reject_non_finite_metadata(
        cls,
        metadata: dict[str, JsonValue],
    ) -> dict[str, JsonValue]:
        _validate_finite_json(metadata)
        return metadata

    def fingerprint(self) -> str:
        canonical_payload = {
            "amount": format(self.amount, ".2f"),
            "currency": self.currency.value,
            "description": self.description,
            "metadata": self.metadata,
            "webhook_url": str(self.webhook_url),
        }
        serialized = json.dumps(
            canonical_payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


class PaymentAccepted(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime


class PaymentDetails(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue] = Field(validation_alias="metadata_")
    status: PaymentStatus
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None


class PaymentCreatedEvent(BaseModel):
    event_id: uuid.UUID
    payment_id: uuid.UUID
    event_type: str
    occurred_at: datetime


class WebhookPayload(BaseModel):
    event_id: uuid.UUID
    payment_id: uuid.UUID
    status: PaymentStatus
    processed_at: datetime
