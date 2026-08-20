from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.database import get_session
from payment_service.errors import (
    IdempotencyConflictError,
    InvalidIdempotencyKeyError,
    PaymentNotFoundError,
)
from payment_service.payments import create_payment, get_payment
from payment_service.schemas import PaymentAccepted, PaymentCreate, PaymentDetails
from payment_service.security import require_api_key

authenticated_router = APIRouter(dependencies=[Depends(require_api_key)])
payments_router = APIRouter(prefix="/api/v1/payments", tags=["payments"])
DatabaseSession = Annotated[AsyncSession, Depends(get_session)]


@payments_router.post(
    "",
    response_model=PaymentAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    responses={409: {"description": "Idempotency key reused with a different request"}},
)
async def create_payment_endpoint(
    payment_data: PaymentCreate,
    idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=255),
    ],
    session: DatabaseSession,
) -> PaymentAccepted:
    try:
        payment = await create_payment(session, payment_data, idempotency_key)
    except InvalidIdempotencyKeyError as error:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Idempotency-Key must contain 1-255 printable characters",
        ) from error
    except IdempotencyConflictError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request",
        ) from error
    return PaymentAccepted.model_validate(payment)


@payments_router.get("/{payment_id}", response_model=PaymentDetails)
async def get_payment_endpoint(
    payment_id: uuid.UUID,
    session: DatabaseSession,
) -> PaymentDetails:
    try:
        payment = await get_payment(session, payment_id)
    except PaymentNotFoundError as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found",
        ) from error
    return PaymentDetails.model_validate(payment)


@authenticated_router.get("/health", status_code=status.HTTP_204_NO_CONTENT)
async def health_endpoint(session: DatabaseSession) -> Response:
    await session.execute(text("SELECT 1"))
    return Response(status_code=status.HTTP_204_NO_CONTENT)


authenticated_router.include_router(payments_router)
