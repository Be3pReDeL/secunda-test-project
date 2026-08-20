from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.types import Lifespan

from payment_service.api import authenticated_router
from payment_service.config import get_settings
from payment_service.database import dispose_engine, get_session_factory
from payment_service.outbox import OutboxRelay
from payment_service.topology import build_topology, create_broker

logger = logging.getLogger(__name__)
OUTBOX_SHUTDOWN_TIMEOUT_SECONDS = 10.0


async def request_validation_exception_handler(
    _: Request,
    error: Exception,
) -> JSONResponse:
    assert isinstance(error, RequestValidationError)
    safe_errors = [
        {
            "type": str(error_detail["type"]),
            "loc": list(error_detail["loc"]),
            "msg": str(error_detail["msg"]),
        }
        for error_detail in error.errors()
    ]
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={"detail": safe_errors},
    )


def create_lifespan(
    *,
    start_outbox_relay: bool,
) -> Lifespan[FastAPI]:
    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        relay: OutboxRelay | None = None
        relay_task: asyncio.Task[None] | None = None
        if start_outbox_relay:
            settings = get_settings()
            relay = OutboxRelay(
                broker=create_broker(settings),
                topology=build_topology(settings),
                session_factory=get_session_factory(),
                settings=settings,
            )
            relay_task = asyncio.create_task(relay.run(), name="outbox-relay")

        try:
            yield
        finally:
            if relay is not None and relay_task is not None:
                relay.stop()
                try:
                    await asyncio.wait_for(
                        relay_task,
                        timeout=OUTBOX_SHUTDOWN_TIMEOUT_SECONDS,
                    )
                except TimeoutError:
                    relay_task.cancel()
                    await asyncio.gather(relay_task, return_exceptions=True)
                except Exception as error:  # noqa: BLE001
                    logger.error(
                        "Outbox relay stopped unexpectedly",
                        extra={"error_type": type(error).__name__},
                    )
            await dispose_engine()

    return lifespan


def create_app(*, start_outbox_relay: bool = True) -> FastAPI:
    application = FastAPI(
        title="Payment Processing Service",
        version="1.0.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=create_lifespan(start_outbox_relay=start_outbox_relay),
    )
    application.add_exception_handler(
        RequestValidationError,
        request_validation_exception_handler,
    )
    application.include_router(authenticated_router)
    return application


app = create_app()
