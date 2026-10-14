"""Unit tests need no external services; integration tests require explicit test URLs."""

import os
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from payment_service.infrastructure.database import PaymentRepository

os.environ.setdefault("API_KEY", "unit-test-api-key-never-use-in-production")


@pytest_asyncio.fixture
async def db() -> AsyncIterator[AsyncEngine]:
    url = os.environ.get("INTEGRATION_DATABASE_URL")
    if not url:
        pytest.skip("INTEGRATION_DATABASE_URL is not configured")
    engine = create_async_engine(url, pool_size=10)
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE outbox, payments CASCADE"))
    yield engine
    await engine.dispose()


@pytest.fixture
def repo(db: AsyncEngine) -> PaymentRepository:
    return PaymentRepository(async_sessionmaker(db, expire_on_commit=False))
