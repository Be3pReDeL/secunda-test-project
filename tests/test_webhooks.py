from __future__ import annotations

import json
import socket
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from payment_service import webhooks
from payment_service.config import Settings
from payment_service.domain import PaymentStatus
from payment_service.errors import WebhookDeliveryError
from payment_service.schemas import WebhookPayload


class FakeResolverLoop:
    def __init__(
        self,
        addresses: list[tuple[Any, ...]] | None = None,
        error: OSError | None = None,
    ) -> None:
        self.addresses = addresses or []
        self.error = error
        self.calls: list[tuple[str, int, int]] = []

    async def getaddrinfo(
        self,
        hostname: str,
        port: int,
        **options: int,
    ) -> list[tuple[Any, ...]]:
        self.calls.append((hostname, port, options["type"]))
        if self.error is not None:
            raise self.error
        return self.addresses


def address_info(ip_address: str, port: int = 443) -> tuple[Any, ...]:
    return (
        socket.AF_INET6 if ":" in ip_address else socket.AF_INET,
        socket.SOCK_STREAM,
        socket.IPPROTO_TCP,
        "",
        (ip_address, port),
    )


@pytest.mark.parametrize(
    "webhook_url",
    [
        "ftp://hooks.example.test/payment",
        "https://user:password@hooks.example.test/payment",
        "https://hooks.example.test:99999/payment",
        "not-a-url",
    ],
)
async def test_validate_webhook_target_rejects_unsupported_urls(webhook_url: str) -> None:
    with pytest.raises(WebhookDeliveryError):
        await webhooks.validate_webhook_target(webhook_url, allow_private=False)


@pytest.mark.parametrize(
    "private_address",
    [
        "127.0.0.1",
        "10.0.0.1",
        "169.254.169.254",
        "::1",
        "fc00::1",
    ],
)
async def test_validate_webhook_target_rejects_non_global_dns_answers(
    monkeypatch: pytest.MonkeyPatch,
    private_address: str,
) -> None:
    resolver = FakeResolverLoop([address_info(private_address)])
    monkeypatch.setattr("payment_service.webhooks.asyncio.get_running_loop", lambda: resolver)

    with pytest.raises(WebhookDeliveryError, match="Private webhook targets are forbidden"):
        await webhooks.validate_webhook_target(
            "https://hooks.example.test/payment",
            allow_private=False,
        )


async def test_validate_webhook_target_rejects_mixed_public_and_private_answers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = FakeResolverLoop(
        [
            address_info("8.8.8.8"),
            address_info("127.0.0.1"),
        ]
    )
    monkeypatch.setattr("payment_service.webhooks.asyncio.get_running_loop", lambda: resolver)

    with pytest.raises(WebhookDeliveryError, match="Private webhook targets are forbidden"):
        await webhooks.validate_webhook_target(
            "https://hooks.example.test/payment",
            allow_private=False,
        )


@pytest.mark.parametrize(
    (
        "webhook_url",
        "resolved_address",
        "expected_port",
        "expected_request_url",
        "expected_host_header",
        "expected_sni_hostname",
    ),
    [
        (
            "https://hooks.example.test/payment?order=42#ignored",
            "8.8.8.8",
            443,
            "https://8.8.8.8/payment?order=42",
            "hooks.example.test",
            "hooks.example.test",
        ),
        (
            "https://hooks.example.test:8443/payment",
            "2606:4700:4700::1111",
            8443,
            "https://[2606:4700:4700::1111]:8443/payment",
            "hooks.example.test:8443",
            "hooks.example.test",
        ),
        (
            "http://hooks.example.test:8080/hook",
            "8.8.4.4",
            8080,
            "http://8.8.4.4:8080/hook",
            "hooks.example.test:8080",
            None,
        ),
    ],
)
async def test_validate_webhook_target_pins_resolved_address(
    monkeypatch: pytest.MonkeyPatch,
    webhook_url: str,
    resolved_address: str,
    expected_port: int,
    expected_request_url: str,
    expected_host_header: str,
    expected_sni_hostname: str | None,
) -> None:
    resolver = FakeResolverLoop([address_info(resolved_address, expected_port)])
    monkeypatch.setattr("payment_service.webhooks.asyncio.get_running_loop", lambda: resolver)

    target = await webhooks.validate_webhook_target(webhook_url, allow_private=False)

    assert target == webhooks.ResolvedWebhookTarget(
        request_url=expected_request_url,
        host_header=expected_host_header,
        sni_hostname=expected_sni_hostname,
    )
    assert resolver.calls == [("hooks.example.test", expected_port, socket.SOCK_STREAM)]


@pytest.mark.parametrize(
    ("resolver", "expected_message"),
    [
        (FakeResolverLoop(), "resolved to no addresses"),
        (FakeResolverLoop(error=OSError("DNS unavailable")), "could not be resolved"),
    ],
)
async def test_validate_webhook_target_rejects_failed_resolution(
    monkeypatch: pytest.MonkeyPatch,
    resolver: FakeResolverLoop,
    expected_message: str,
) -> None:
    monkeypatch.setattr("payment_service.webhooks.asyncio.get_running_loop", lambda: resolver)

    with pytest.raises(WebhookDeliveryError, match=expected_message):
        await webhooks.validate_webhook_target(
            "https://hooks.example.test/payment",
            allow_private=False,
        )


async def test_allow_private_still_resolves_and_pins_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = FakeResolverLoop([address_info("127.0.0.1", 8080)])
    monkeypatch.setattr("payment_service.webhooks.asyncio.get_running_loop", lambda: resolver)

    target = await webhooks.validate_webhook_target(
        "http://localhost:8080/hook",
        allow_private=True,
    )

    assert target == webhooks.ResolvedWebhookTarget(
        request_url="http://127.0.0.1:8080/hook",
        host_header="localhost:8080",
        sni_hostname=None,
    )
    assert resolver.calls == [("localhost", 8080, socket.SOCK_STREAM)]


def make_payload(event_id: uuid.UUID) -> WebhookPayload:
    return WebhookPayload(
        event_id=event_id,
        payment_id=uuid.uuid4(),
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
    )


def make_resolved_target(*, sni_hostname: str | None) -> webhooks.ResolvedWebhookTarget:
    return webhooks.ResolvedWebhookTarget(
        request_url=(
            "https://8.8.8.8/payment" if sni_hostname else "http://8.8.8.8/payment"
        ),
        host_header="hooks.example.test",
        sni_hostname=sni_hostname,
    )


async def test_http_sender_sends_pinned_idempotent_https_request(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
) -> None:
    client = SimpleNamespace(
        send=AsyncMock(return_value=httpx.Response(204)),
        aclose=AsyncMock(),
    )
    client_factory = Mock(return_value=client)
    target = make_resolved_target(sni_hostname="hooks.example.test")
    validate_target = AsyncMock(return_value=target)
    monkeypatch.setattr("payment_service.webhooks.httpx.AsyncClient", client_factory)
    monkeypatch.setattr(webhooks, "validate_webhook_target", validate_target)
    sender = webhooks.HttpWebhookSender(settings)
    event_id = uuid.uuid4()
    payload = make_payload(event_id)

    await sender.send(
        "https://hooks.example.test/payment",
        payload,
        event_id=event_id,
    )
    await sender.close()

    validate_target.assert_awaited_once_with(
        "https://hooks.example.test/payment",
        allow_private=False,
    )
    client.send.assert_awaited_once()
    request = client.send.await_args.args[0]
    assert isinstance(request, httpx.Request)
    assert request.method == "POST"
    assert str(request.url) == target.request_url
    assert request.headers["Host"] == target.host_header
    assert request.headers["Idempotency-Key"] == str(event_id)
    assert request.headers["X-Webhook-Event-ID"] == str(event_id)
    assert request.extensions["sni_hostname"] == "hooks.example.test"
    assert json.loads(request.content) == payload.model_dump(mode="json")
    constructor_arguments = client_factory.call_args.kwargs
    assert constructor_arguments["follow_redirects"] is False
    assert constructor_arguments["trust_env"] is False
    assert constructor_arguments["limits"].max_keepalive_connections == 0
    client.aclose.assert_awaited_once_with()


async def test_http_sender_omits_sni_for_http_target(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
) -> None:
    client = SimpleNamespace(
        send=AsyncMock(return_value=httpx.Response(204)),
        aclose=AsyncMock(),
    )
    target = make_resolved_target(sni_hostname=None)
    monkeypatch.setattr("payment_service.webhooks.httpx.AsyncClient", lambda **_: client)
    monkeypatch.setattr(
        webhooks,
        "validate_webhook_target",
        AsyncMock(return_value=target),
    )
    sender = webhooks.HttpWebhookSender(settings)
    event_id = uuid.uuid4()

    await sender.send(
        "http://hooks.example.test/payment",
        make_payload(event_id),
        event_id=event_id,
    )
    await sender.close()

    request = client.send.await_args.args[0]
    assert str(request.url) == target.request_url
    assert request.headers["Host"] == target.host_header
    assert "sni_hostname" not in request.extensions


@pytest.mark.parametrize("status_code", [300, 400, 429, 500])
async def test_http_sender_rejects_non_success_status(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
    status_code: int,
) -> None:
    client = SimpleNamespace(
        send=AsyncMock(return_value=httpx.Response(status_code)),
        aclose=AsyncMock(),
    )
    monkeypatch.setattr("payment_service.webhooks.httpx.AsyncClient", lambda **_: client)
    monkeypatch.setattr(
        webhooks,
        "validate_webhook_target",
        AsyncMock(return_value=make_resolved_target(sni_hostname="hooks.example.test")),
    )
    sender = webhooks.HttpWebhookSender(settings)
    event_id = uuid.uuid4()

    with pytest.raises(WebhookDeliveryError, match=f"HTTP {status_code}"):
        await sender.send(
            "https://hooks.example.test/payment",
            make_payload(event_id),
            event_id=event_id,
        )
    await sender.close()


async def test_http_sender_translates_httpx_errors_without_leaking_message(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
) -> None:
    client = SimpleNamespace(
        send=AsyncMock(side_effect=httpx.ConnectError("secret upstream detail")),
        aclose=AsyncMock(),
    )
    monkeypatch.setattr("payment_service.webhooks.httpx.AsyncClient", lambda **_: client)
    monkeypatch.setattr(
        webhooks,
        "validate_webhook_target",
        AsyncMock(return_value=make_resolved_target(sni_hostname="hooks.example.test")),
    )
    sender = webhooks.HttpWebhookSender(settings)
    event_id = uuid.uuid4()

    with pytest.raises(WebhookDeliveryError, match=r"^ConnectError$"):
        await sender.send(
            "https://hooks.example.test/payment",
            make_payload(event_id),
            event_id=event_id,
        )
    await sender.close()
