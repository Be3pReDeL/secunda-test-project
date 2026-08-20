from __future__ import annotations

import asyncio
import ipaddress
import socket
import uuid
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

import httpx

from payment_service.config import Settings
from payment_service.errors import WebhookDeliveryError
from payment_service.schemas import WebhookPayload


@dataclass(frozen=True, slots=True)
class ResolvedWebhookTarget:
    request_url: str
    host_header: str
    sni_hostname: str | None


async def validate_webhook_target(
    webhook_url: str,
    *,
    allow_private: bool,
) -> ResolvedWebhookTarget:
    parsed_url = urlsplit(webhook_url)
    if parsed_url.scheme not in {"http", "https"} or parsed_url.hostname is None:
        raise WebhookDeliveryError("Unsupported webhook URL")
    if parsed_url.username is not None or parsed_url.password is not None:
        raise WebhookDeliveryError("Webhook URL credentials are forbidden")

    try:
        parsed_port = parsed_url.port
    except ValueError as error:
        raise WebhookDeliveryError("Unsupported webhook URL") from error
    port = parsed_port or (443 if parsed_url.scheme == "https" else 80)
    try:
        address_info = await asyncio.get_running_loop().getaddrinfo(
            parsed_url.hostname,
            port,
            type=socket.SOCK_STREAM,
        )
    except OSError as error:
        raise WebhookDeliveryError("Webhook hostname could not be resolved") from error

    if not address_info:
        raise WebhookDeliveryError("Webhook hostname resolved to no addresses")

    resolved_addresses: list[ipaddress.IPv4Address | ipaddress.IPv6Address] = []
    for *_, socket_address in address_info:
        ip_address = ipaddress.ip_address(socket_address[0])
        if not allow_private and not ip_address.is_global:
            raise WebhookDeliveryError("Private webhook targets are forbidden")
        resolved_addresses.append(ip_address)

    selected_address = resolved_addresses[0]
    address_literal = str(selected_address)
    if selected_address.version == 6:
        address_literal = f"[{address_literal}]"
    if parsed_port is not None:
        address_literal = f"{address_literal}:{parsed_port}"

    request_url = urlunsplit(
        (
            parsed_url.scheme,
            address_literal,
            parsed_url.path,
            parsed_url.query,
            "",
        )
    )
    return ResolvedWebhookTarget(
        request_url=request_url,
        host_header=parsed_url.netloc,
        sni_hostname=parsed_url.hostname if parsed_url.scheme == "https" else None,
    )


class HttpWebhookSender:
    def __init__(self, settings: Settings) -> None:
        self._allow_private = settings.allow_private_webhooks
        self._client = httpx.AsyncClient(
            follow_redirects=False,
            limits=httpx.Limits(max_keepalive_connections=0),
            timeout=settings.webhook_timeout_seconds,
            trust_env=False,
        )

    async def send(
        self,
        webhook_url: str,
        payload: WebhookPayload,
        *,
        event_id: uuid.UUID,
    ) -> None:
        target = await validate_webhook_target(
            webhook_url,
            allow_private=self._allow_private,
        )
        request_extensions = (
            {"sni_hostname": target.sni_hostname} if target.sni_hostname is not None else None
        )
        request = httpx.Request(
            "POST",
            target.request_url,
            json=payload.model_dump(mode="json"),
            headers={
                "Host": target.host_header,
                "Idempotency-Key": str(event_id),
                "X-Webhook-Event-ID": str(event_id),
            },
            extensions=request_extensions,
        )
        try:
            response = await self._client.send(request)
        except httpx.HTTPError as error:
            raise WebhookDeliveryError(type(error).__name__) from error
        if not response.is_success:
            raise WebhookDeliveryError(f"Webhook returned HTTP {response.status_code}")

    async def close(self) -> None:
        await self._client.aclose()
