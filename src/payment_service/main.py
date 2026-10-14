"""FastAPI application factory; all routes including documentation require the API key."""

import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Security
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import APIKeyHeader
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker

from payment_service.api.schemas import CreatePayment, PaymentAccepted, PaymentDetails
from payment_service.config import Settings
from payment_service.domain import IdempotencyConflict, PaymentNotFound
from payment_service.infrastructure.database import PaymentRepository, create_engine
from payment_service.infrastructure.logging import configure_logging
from payment_service.infrastructure.webhook import validate_webhook_url


def create_app(settings: Settings | None = None) -> FastAPI:
    config = settings or Settings()
    configure_logging(config.log_level)
    engine = create_engine(config)
    repository = PaymentRepository(async_sessionmaker(engine, expire_on_commit=False))
    key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

    async def require_key(key: Annotated[str | None, Security(key_header)]) -> None:
        expected = config.api_key.get_secret_value().encode()
        if key is None or not secrets.compare_digest(key.encode(), expected):
            raise HTTPException(status_code=401, detail="Invalid or missing API key")

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        await engine.dispose()

    application = FastAPI(
        title="Asynchronous Payment Service",
        version="1.0.0",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        dependencies=[Depends(require_key)],
    )
    application.state.engine = engine
    application.state.repository = repository

    @application.exception_handler(RequestValidationError)
    async def invalid_request(_: Request, error: RequestValidationError) -> JSONResponse:
        # Do not echo untrusted input/exception objects. In particular NaN accepted
        # by Python's JSON decoder must produce 422, not a JSON serialization 500.
        detail = [
            {"loc": item["loc"], "msg": item["msg"], "type": item["type"]}
            for item in error.errors()
        ]
        return JSONResponse(status_code=422, content={"detail": detail})

    @application.exception_handler(IdempotencyConflict)
    async def conflict(_: Request, __: IdempotencyConflict) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": "Idempotency key payload conflict"})

    @application.exception_handler(PaymentNotFound)
    async def missing(_: Request, __: PaymentNotFound) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": "Payment not found"})

    @application.exception_handler(SQLAlchemyError)
    @application.exception_handler(OSError)
    @application.exception_handler(TimeoutError)
    async def unavailable(_: Request, __: Exception) -> JSONResponse:
        return JSONResponse(status_code=503, content={"detail": "Database unavailable"})

    @application.post("/api/v1/payments", status_code=202, response_model=PaymentAccepted)
    async def create_payment(
        body: CreatePayment,
        idempotency_key: Annotated[str, Header(min_length=1, max_length=200)],
    ) -> PaymentAccepted:
        if not idempotency_key.strip():
            raise HTTPException(status_code=422, detail="Idempotency-Key cannot be blank")
        try:
            normalized_url = validate_webhook_url(
                body.webhook_url, allowed_hosts=config.webhook_allowed_hosts
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="Invalid or unsafe webhook URL") from exc
        payment = await repository.create(body.command(normalized_url), idempotency_key)
        return PaymentAccepted(
            payment_id=payment.id, status=payment.status, created_at=payment.created_at
        )

    @application.get("/api/v1/payments/{payment_id}", response_model=PaymentDetails)
    async def get_payment(payment_id: UUID) -> PaymentDetails:
        return PaymentDetails.model_validate(await repository.get(payment_id))

    @application.get("/health")
    async def health() -> dict[str, str]:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        return {"status": "ok"}

    @application.get("/openapi.json", include_in_schema=False)
    async def openapi() -> JSONResponse:
        return JSONResponse(application.openapi())

    @application.get("/docs", include_in_schema=False)
    async def docs() -> HTMLResponse:
        return get_swagger_ui_html(openapi_url="/openapi.json", title="Payment API")

    return application


app = create_app()
