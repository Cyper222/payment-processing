"""API contracts backed by real PostgreSQL, including concurrent insert races."""

import asyncio
import os
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from payment_service.config import Settings
from payment_service.infrastructure.database import PaymentRepository
from payment_service.infrastructure.models import Outbox, PaymentRow
from payment_service.main import create_app

pytestmark = pytest.mark.integration
KEY = "integration-api-key-not-for-production"
BODY = {
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order 42",
    "metadata": {"order": 42},
    "webhook_url": "https://example.com/events",
}


@pytest_asyncio.fixture
async def client(db: AsyncEngine) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(
        Settings(
            database_url=os.environ["INTEGRATION_DATABASE_URL"],
            api_key=KEY,
            webhook_allowed_hosts=[],
        )
    )
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
            headers={"X-API-Key": KEY, "Idempotency-Key": "test-order"},
        ) as client:
            yield client


async def test_create_get_and_not_found(client: httpx.AsyncClient) -> None:
    response = await client.post("/api/v1/payments", json=BODY)
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "pending"
    assert data["created_at"].endswith("Z") or "+00:00" in data["created_at"]
    detail = await client.get(f"/api/v1/payments/{data['payment_id']}")
    assert detail.status_code == 200
    assert detail.json()["amount"] == "1500.00"
    assert detail.json()["metadata"] == {"order": 42}
    assert detail.json()["processed_at"] is None
    assert (await client.get(f"/api/v1/payments/{uuid4()} ".strip())).status_code == 404
    assert (await client.get("/api/v1/payments/not-uuid")).status_code == 422


@pytest.mark.parametrize("path", ["/health", "/docs", "/openapi.json", "/api/v1/payments/abc"])
async def test_auth_all_routes(client: httpx.AsyncClient, path: str) -> None:
    client.headers.pop("X-API-Key")
    assert (await client.get(path)).status_code == 401
    assert (await client.get(path, headers={"X-API-Key": "wrong"})).status_code == 401


@pytest.mark.parametrize(
    "patch",
    [
        {"amount": "0"},
        {"amount": "-2"},
        {"amount": "1.001"},
        {"amount": True},
        {"amount": "10000000000000000"},
        {"amount": "NaN"},
        {"currency": "GBP"},
        {"webhook_url": "file:///etc/passwd"},
        {"webhook_url": "not-a-url"},
        {"webhook_url": "http://127.0.0.1/test"},
        {"webhook_url": "http://169.254.169.254/"},
        {"webhook_url": "https://user:secret@example.com"},
        {"unknown": True},
    ],
)
async def test_validation(client: httpx.AsyncClient, patch: dict[str, object]) -> None:
    assert (await client.post("/api/v1/payments", json={**BODY, **patch})).status_code == 422


async def test_required_key_and_bad_json(client: httpx.AsyncClient) -> None:
    client.headers.pop("Idempotency-Key")
    assert (await client.post("/api/v1/payments", json=BODY)).status_code == 422
    client.headers["Idempotency-Key"] = "   "
    assert (await client.post("/api/v1/payments", json=BODY)).status_code == 422
    assert (await client.post("/api/v1/payments", content=b"{oops")).status_code == 422


async def test_idempotency_and_conflict(client: httpx.AsyncClient, db: AsyncEngine) -> None:
    first = await client.post("/api/v1/payments", json=BODY)
    second = await client.post("/api/v1/payments", json={**BODY, "amount": "1500"})
    assert first.json() == second.json()
    assert (await client.post("/api/v1/payments", json={**BODY, "amount": "2"})).status_code == 409
    async with async_sessionmaker(db)() as session:
        assert await session.scalar(select(func.count()).select_from(PaymentRow)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_concurrent_idempotency(client: httpx.AsyncClient, db: AsyncEngine) -> None:
    responses = await asyncio.gather(
        *[client.post("/api/v1/payments", json=BODY) for _ in range(10)]
    )
    assert {response.status_code for response in responses} == {202}
    assert len({response.json()["payment_id"] for response in responses}) == 1
    async with async_sessionmaker(db)() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_atomic_rollback_when_outbox_insert_fails(
    client: httpx.AsyncClient, db: AsyncEngine
) -> None:
    async with db.begin() as connection:
        await connection.execute(
            text("""
            CREATE FUNCTION test_reject_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected outbox failure'; END $$
        """)
        )
        await connection.execute(
            text("""
            CREATE TRIGGER test_fail BEFORE INSERT ON outbox
            FOR EACH ROW EXECUTE FUNCTION test_reject_outbox()
        """)
        )
    try:
        response = await client.post("/api/v1/payments", json=BODY)
        assert response.status_code == 503
        async with async_sessionmaker(db)() as session:
            assert await session.scalar(select(func.count()).select_from(PaymentRow)) == 0
            assert await session.scalar(select(func.count()).select_from(Outbox)) == 0
    finally:
        async with db.begin() as connection:
            await connection.execute(text("DROP TRIGGER test_fail ON outbox"))
            await connection.execute(text("DROP FUNCTION test_reject_outbox()"))


async def test_openapi(client: httpx.AsyncClient) -> None:
    response = await client.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    assert schema["components"]["securitySchemes"]["APIKeyHeader"]["name"] == "X-API-Key"
    headers = schema["paths"]["/api/v1/payments"]["post"]["parameters"]
    assert any(item["name"] == "idempotency-key" and item["required"] for item in headers)


@pytest.mark.parametrize(
    "failure",
    [
        ConnectionRefusedError("private connection details"),
        TimeoutError("private connection details"),
    ],
)
async def test_connection_failures_return_safe_503(client, monkeypatch, failure):
    monkeypatch.setattr(PaymentRepository, "create", AsyncMock(side_effect=failure))
    response = await client.post("/api/v1/payments", json=BODY)
    assert response.status_code == 503
    assert response.json() == {"detail": "Database unavailable"}


@pytest.mark.parametrize("invalid_number", ["NaN", "Infinity", "-Infinity"])
async def test_nonfinite_raw_json_returns_422(client, invalid_number):
    body = '{"amount":' + invalid_number + ',"currency":"RUB","webhook_url":"https://example.com"}'
    response = await client.post(
        "/api/v1/payments", content=body, headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 422
