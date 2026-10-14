"""Outbox guarantees with real transactions and injected publisher crash windows."""

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker

from payment_service.application.processing import ProcessingService
from payment_service.domain import Currency, NewPayment, PaymentEvent, PaymentStatus
from payment_service.infrastructure.database import SQLPaymentStore
from payment_service.infrastructure.models import Outbox
from payment_service.infrastructure.outbox import OutboxDispatcher

pytestmark = pytest.mark.integration


async def create(repo):
    return await repo.create(
        NewPayment(Decimal("25.00"), Currency.USD, "Outbox", {}, "https://example.com/hook"),
        str(uuid4()),
    )


async def records(db):
    async with async_sessionmaker(db)() as session:
        return list(await session.scalars(select(Outbox)))


async def test_broker_failure_preserves_pending_then_recovers(db, repo):
    await create(repo)
    publisher = Mock(publish=AsyncMock(side_effect=ConnectionError))
    dispatcher = OutboxDispatcher(async_sessionmaker(db, expire_on_commit=False), publisher)
    with pytest.raises(ConnectionError):
        await dispatcher.dispatch_once()
    assert (await records(db))[0].published_at is None
    publisher.publish.side_effect = None
    assert await dispatcher.dispatch_once()
    assert (await records(db))[0].published_at is not None
    assert not await dispatcher.dispatch_once()
    assert publisher.publish.await_count == 2


async def test_crash_after_confirm_before_commit_republishes_without_recharge(db, repo):
    payment = await create(repo)
    published = []

    async def confirm(payload, destination, message_id):
        published.append(PaymentEvent.model_validate(payload))

    publisher = Mock(publish=AsyncMock(side_effect=confirm))
    dispatcher = OutboxDispatcher(async_sessionmaker(db, expire_on_commit=False), publisher)
    async with db.begin() as connection:
        await connection.execute(
            text("""
            CREATE FUNCTION test_fail_published() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected crash after broker confirm'; END $$
        """)
        )
        await connection.execute(
            text("""
            CREATE TRIGGER test_fail_publish BEFORE UPDATE ON outbox
            FOR EACH ROW EXECUTE FUNCTION test_fail_published()
        """)
        )
    try:
        with pytest.raises(DBAPIError):
            await dispatcher.dispatch_once()
    finally:
        async with db.begin() as connection:
            await connection.execute(text("DROP TRIGGER test_fail_publish ON outbox"))
            await connection.execute(text("DROP FUNCTION test_fail_published()"))
    assert len(published) == 1
    assert (await records(db))[0].published_at is None
    await dispatcher.dispatch_once()
    assert len(published) == 2
    assert published[0] == published[1]
    gateway = Mock(charge=AsyncMock(return_value=PaymentStatus.SUCCEEDED))
    webhook = Mock(send=AsyncMock())
    service = ProcessingService(SQLPaymentStore(db), gateway, webhook)
    await service.process(published[0])
    await service.process(published[1])
    gateway.charge.assert_awaited_once()
    webhook.send.assert_awaited_once()
    assert (await repo.get(payment.id)).webhook_delivered_at is not None


async def test_dispatchers_skip_locked_rows(db, repo):
    await create(repo)
    await create(repo)
    entered = asyncio.Event()
    release = asyncio.Event()
    sent = []

    async def hold_first(payload, destination, message_id):
        sent.append(message_id)
        entered.set()
        await release.wait()

    sessions = async_sessionmaker(db, expire_on_commit=False)
    first = OutboxDispatcher(sessions, Mock(publish=AsyncMock(side_effect=hold_first)))
    second_publisher = Mock(publish=AsyncMock())
    second = OutboxDispatcher(sessions, second_publisher)
    running = asyncio.create_task(first.dispatch_once())
    try:
        await asyncio.wait_for(entered.wait(), 3)
        assert await asyncio.wait_for(second.dispatch_once(), 3)
        assert second_publisher.publish.call_args.args[2] != sent[0]
    finally:
        release.set()
        await running
    assert all(row.published_at for row in await records(db))
