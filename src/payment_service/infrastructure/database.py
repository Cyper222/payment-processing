"""Atomic persistence and per-payment session locks without long transactions."""

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from payment_service.config import Settings
from payment_service.domain import (
    Currency,
    IdempotencyConflict,
    NewPayment,
    Payment,
    PaymentEvent,
    PaymentNotFound,
    PaymentStatus,
)
from payment_service.infrastructure.models import Outbox, PaymentRow


def create_engine(settings: Settings) -> AsyncEngine:
    return create_async_engine(settings.database_url, pool_pre_ping=True, pool_size=10)


def fingerprint(command: NewPayment) -> str:
    normalized = {
        "amount": format(command.amount, ".2f"),
        "currency": command.currency.value,
        "description": command.description,
        "metadata": command.metadata,
        "webhook_url": command.webhook_url,
    }
    value = json.dumps(normalized, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(value.encode()).hexdigest()


def to_payment(row: PaymentRow) -> Payment:
    return Payment(
        id=row.id,
        amount=row.amount,
        currency=Currency(row.currency),
        description=row.description,
        metadata=row.metadata_json,
        webhook_url=row.webhook_url,
        status=PaymentStatus(row.status),
        idempotency_key=row.idempotency_key,
        created_at=row.created_at,
        processed_at=row.processed_at,
        webhook_delivered_at=row.webhook_delivered_at,
        completed_attempt=row.completed_attempt,
        last_error=row.last_error,
        processing_event_id=row.processing_event_id,
    )


def outbox_record(event: PaymentEvent, destination: str) -> Outbox:
    return Outbox(
        payment_id=event.payment_id,
        event_id=event.event_id,
        attempt=event.attempt,
        destination=destination,
        payload=event.model_dump(mode="json"),
    )


class PaymentRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self.sessions = sessions

    async def create(self, command: NewPayment, key: str) -> Payment:
        digest = fingerprint(command)
        event_id = uuid4()
        async with self.sessions() as session, session.begin():
            # ON CONFLICT waits for an uncommitted competing insertion. READ COMMITTED
            # then sees the winner in the subsequent SELECT; no failed transaction leaks.
            payment_id = await session.scalar(
                insert(PaymentRow)
                .values(
                    id=uuid4(),
                    amount=command.amount,
                    currency=command.currency.value,
                    description=command.description,
                    metadata_json=command.metadata,
                    status=PaymentStatus.PENDING.value,
                    idempotency_key=key,
                    request_fingerprint=digest,
                    processing_event_id=event_id,
                    webhook_url=command.webhook_url,
                )
                .on_conflict_do_nothing(constraint="uq_payments_idempotency_key")
                .returning(PaymentRow.id)
            )
            row = (
                await session.scalars(select(PaymentRow).where(PaymentRow.idempotency_key == key))
            ).one()
            if row.request_fingerprint != digest:
                raise IdempotencyConflict
            if payment_id is not None:
                event = PaymentEvent(event_id=event_id, payment_id=payment_id)
                session.add(outbox_record(event, "payments.new"))
                await session.flush()
            return to_payment(row)

    async def get(self, payment_id: UUID) -> Payment:
        async with self.sessions() as session:
            row = await session.get(PaymentRow, payment_id)
            if row is None:
                raise PaymentNotFound
            return to_payment(row)


class SQLProcessingRepository:
    def __init__(self, session: AsyncSession, connection: AsyncConnection) -> None:
        self.session = session
        self.connection = connection

    async def get(self, payment_id: UUID) -> Payment:
        async with self.session.begin():
            row = await self.session.get(PaymentRow, payment_id, populate_existing=True)
            if row is None:
                raise PaymentNotFound
            return to_payment(row)

    async def save_result(self, payment_id: UUID, status: PaymentStatus) -> Payment:
        async with self.session.begin():
            await self.session.execute(
                update(PaymentRow)
                .where(
                    PaymentRow.id == payment_id, PaymentRow.status == PaymentStatus.PENDING.value
                )
                .values(status=status.value, processed_at=datetime.now(UTC))
            )
        return await self.get(payment_id)

    async def mark_delivered(self, event: PaymentEvent) -> None:
        async with self.session.begin():
            await self.session.execute(
                update(PaymentRow)
                .where(PaymentRow.id == event.payment_id)
                .values(
                    webhook_delivered_at=datetime.now(UTC),
                    completed_attempt=event.attempt,
                    last_error=None,
                )
            )

    async def schedule_failure(self, event: PaymentEvent, error: str) -> None:
        # A reconnection would have lost the session advisory lock. Let the AMQP
        # fallback preserve this delivery instead of writing progress without a lock.
        if self.connection.invalidated or self.connection.closed:
            raise ConnectionError("Payment lock connection was lost")
        if event.attempt < 3:
            destination = f"payments.retry.{2**event.attempt}s"
            next_event = event.model_copy(update={"attempt": event.attempt + 1, "error": error})
        else:
            destination = "payments.dlq"
            next_event = event.model_copy(update={"error": error})
        async with self.session.begin():
            await self.session.execute(
                update(PaymentRow)
                .where(PaymentRow.id == event.payment_id)
                .values(completed_attempt=event.attempt, last_error=error)
            )
            values = outbox_record(next_event, destination)
            await self.session.execute(
                insert(Outbox)
                .values(
                    payment_id=values.payment_id,
                    event_id=values.event_id,
                    attempt=values.attempt,
                    destination=values.destination,
                    payload=values.payload,
                )
                .on_conflict_do_nothing(constraint="uq_outbox_delivery")
            )


class SQLPaymentStore:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    @asynccontextmanager
    async def locked(self, payment_id: UUID) -> AsyncIterator[SQLProcessingRepository]:
        # A hash collision only serializes two unrelated payments; it cannot mix data.
        lock_id = int.from_bytes(payment_id.bytes[:8], "big", signed=True)
        async with self.engine.connect() as connection:
            try:
                await connection.execute(text("SELECT pg_advisory_lock(:key)"), {"key": lock_id})
                await connection.commit()
                async with AsyncSession(bind=connection, expire_on_commit=False) as session:
                    yield SQLProcessingRepository(session, connection)
            finally:
                # Drop the physical session, also on cancellation. A session-level lock
                # must never return to the connection pool, even after a network failure.
                await asyncio.shield(connection.invalidate())
