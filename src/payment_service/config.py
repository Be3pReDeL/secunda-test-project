from __future__ import annotations

from functools import lru_cache

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="PAYMENTS_",
        extra="ignore",
        allow_inf_nan=False,
    )

    api_key: SecretStr = Field(min_length=16)
    database_url: str = Field(min_length=1)
    rabbitmq_url: str = Field(min_length=1)
    allow_private_webhooks: bool = False

    gateway_min_delay_seconds: float = Field(default=2.0, ge=0)
    gateway_max_delay_seconds: float = Field(default=5.0, ge=0)
    gateway_success_probability: float = Field(default=0.9, ge=0, le=1)

    retry_base_delay_seconds: float = Field(default=1.0, gt=0)
    webhook_timeout_seconds: float = Field(default=5.0, gt=0)
    broker_publish_timeout_seconds: float = Field(default=5.0, gt=0)
    broker_reconnect_interval_seconds: float = Field(default=2.0, gt=0)

    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_poll_interval_seconds: float = Field(default=0.5, gt=0)
    outbox_lease_seconds: float = Field(default=30.0, gt=0)
    outbox_max_backoff_seconds: float = Field(default=60.0, gt=0)

    @model_validator(mode="after")
    def validate_delay_range(self) -> Settings:
        if self.gateway_max_delay_seconds < self.gateway_min_delay_seconds:
            raise ValueError("gateway_max_delay_seconds must be >= gateway_min_delay_seconds")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
