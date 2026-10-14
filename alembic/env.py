"""Run Alembic through an async SQLAlchemy connection."""

import asyncio
import os

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from alembic import context
from payment_service.infrastructure.models import Base

target_metadata = Base.metadata


def database_url() -> str:
    from dotenv import load_dotenv

    load_dotenv()
    return os.environ["DATABASE_URL"]


def run_sync(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_online() -> None:
    engine = create_async_engine(database_url(), poolclass=pool.NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(run_sync)
    await engine.dispose()


if context.is_offline_mode():
    context.configure(url=database_url(), target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    asyncio.run(run_online())
