"""Real PostgreSQL transactions/locks with deterministic external adapters."""

import asyncio
import os
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from payment_service.application.processing import ProcessingService
from payment_service.domain import Currency, NewPayment, PaymentEvent, PaymentStatus
from payment_service.infrastructure.database import SQLPaymentStore
from payment_service.infrastructure.models import Outbox
from payment_service.infrastructure.webhook import WebhookDeliveryError
from payment_service.replay import replay

pytestmark = pytest.mark.integration


async def payment_and_event(db, repo):
    payment = await repo.create(
        NewPayment(
            amount=Decimal("12.50"),
            currency=Currency.RUB,
            description="processing integration",
            metadata={"test": True},
            webhook_url="https://merchant.example/hook",
        ),
        str(uuid4()),
    )
    async with async_sessionmaker(db)() as session:
        record = (
            await session.scalars(select(Outbox).where(Outbox.payment_id == payment.id))
        ).one()
        return payment, PaymentEvent.model_validate(record.payload)


async def delivery_records(db, payment_id):
    async with async_sessionmaker(db)() as session:
        return list(
            await session.scalars(
                select(Outbox)
                .where(Outbox.payment_id == payment_id)
                .order_by(Outbox.created_at, Outbox.id)
            )
        )


async def test_result_is_committed_before_webhook(db, repo):
    payment, event = await payment_and_event(db, repo)
    gateway = AsyncMock()
    gateway.charge.return_value = PaymentStatus.SUCCEEDED

    async def verify_committed(notification):
        persisted = await repo.get(payment.id)
        assert persisted.status is PaymentStatus.SUCCEEDED
        assert persisted.processed_at is not None
        assert persisted.processed_at == notification.processed_at
        assert persisted.webhook_delivered_at is None

    webhook = AsyncMock()
    webhook.send.side_effect = verify_committed
    await ProcessingService(SQLPaymentStore(db), gateway, webhook).process(event)

    persisted = await repo.get(payment.id)
    assert persisted.status is PaymentStatus.SUCCEEDED
    assert persisted.webhook_delivered_at is not None
    assert persisted.completed_attempt == 1
    gateway.charge.assert_awaited_once()
    webhook.send.assert_awaited_once()


async def test_concurrent_duplicate_messages_only_charge_once(db, repo):
    payment, event = await payment_and_event(db, repo)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def delayed_result(payment):
        entered.set()
        await release.wait()
        return PaymentStatus.SUCCEEDED

    gateway = AsyncMock()
    gateway.charge.side_effect = delayed_result
    webhook = AsyncMock()
    service = ProcessingService(SQLPaymentStore(db), gateway, webhook)
    first = asyncio.create_task(service.process(event))
    await asyncio.wait_for(entered.wait(), 3)
    second = asyncio.create_task(service.process(event))
    release.set()
    await asyncio.wait_for(asyncio.gather(first, second), 5)

    gateway.charge.assert_awaited_once()
    webhook.send.assert_awaited_once()
    assert (await repo.get(payment.id)).webhook_delivered_at is not None
    assert len(await delivery_records(db, payment.id)) == 1


async def test_business_decline_is_terminal_and_not_retried(db, repo):
    payment, event = await payment_and_event(db, repo)
    gateway = AsyncMock()
    gateway.charge.return_value = PaymentStatus.FAILED
    webhook = AsyncMock()
    service = ProcessingService(SQLPaymentStore(db), gateway, webhook)

    await service.process(event)
    await service.process(event)

    persisted = await repo.get(payment.id)
    assert persisted.status is PaymentStatus.FAILED
    assert persisted.processed_at is not None
    assert persisted.webhook_delivered_at is not None
    gateway.charge.assert_awaited_once()
    webhook.send.assert_awaited_once()
    assert webhook.send.call_args.args[0].status is PaymentStatus.FAILED
    assert len(await delivery_records(db, payment.id)) == 1


async def test_three_failed_webhooks_schedule_2s_4s_then_dlq_without_recharging(db, repo):
    payment, event = await payment_and_event(db, repo)
    gateway = AsyncMock()
    gateway.charge.return_value = PaymentStatus.SUCCEEDED
    webhook = AsyncMock()
    webhook.send.side_effect = WebhookDeliveryError("receiver unavailable")
    service = ProcessingService(SQLPaymentStore(db), gateway, webhook)
    original = event
    processed_at = None

    for attempt, destination in enumerate(
        ("payments.retry.2s", "payments.retry.4s", "payments.dlq"), start=1
    ):
        await service.process(event)
        persisted = await repo.get(payment.id)
        assert persisted.status is PaymentStatus.SUCCEEDED
        assert persisted.webhook_delivered_at is None
        assert persisted.completed_attempt == attempt
        assert persisted.last_error == "WebhookDeliveryError"
        processed_at = processed_at or persisted.processed_at
        assert persisted.processed_at == processed_at
        records = await delivery_records(db, payment.id)
        assert len(records) == attempt + 1
        assert records[-1].destination == destination
        event = PaymentEvent.model_validate(records[-1].payload)
        assert event.event_id == original.event_id
        assert event.attempt == min(attempt + 1, 3)
        assert event.error == "WebhookDeliveryError"
        # An earlier publication duplicate cannot consume another attempt or
        # manufacture another retry row after the decision was committed.
        await service.process(original)
        assert len(await delivery_records(db, payment.id)) == attempt + 1
        assert webhook.send.await_count == attempt

    await service.process(event)
    assert webhook.send.await_count == 3
    gateway.charge.assert_awaited_once()
    assert len(await delivery_records(db, payment.id)) == 4


async def test_concurrent_failed_duplicates_create_one_retry(db, repo):
    payment, event = await payment_and_event(db, repo)
    gateway = AsyncMock()
    gateway.charge.return_value = PaymentStatus.SUCCEEDED
    webhook = AsyncMock()
    webhook.send.side_effect = WebhookDeliveryError("receiver unavailable")
    service = ProcessingService(SQLPaymentStore(db), gateway, webhook)

    await asyncio.wait_for(asyncio.gather(service.process(event), service.process(event)), 5)

    gateway.charge.assert_awaited_once()
    webhook.send.assert_awaited_once()
    records = await delivery_records(db, payment.id)
    assert [record.destination for record in records] == ["payments.new", "payments.retry.2s"]
    assert (await repo.get(payment.id)).completed_attempt == 1


async def test_cancellation_after_result_commit_resumes_webhook_without_recharge(db, repo):
    payment, event = await payment_and_event(db, repo)
    gateway = AsyncMock()
    gateway.charge.return_value = PaymentStatus.SUCCEEDED
    entered = asyncio.Event()

    async def interrupted_delivery(payment):
        entered.set()
        await asyncio.Future()

    webhook = AsyncMock()
    webhook.send.side_effect = interrupted_delivery
    task = asyncio.create_task(
        ProcessingService(SQLPaymentStore(db), gateway, webhook).process(event)
    )
    await asyncio.wait_for(entered.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    persisted = await repo.get(payment.id)
    assert persisted.status is PaymentStatus.SUCCEEDED
    assert persisted.processed_at is not None
    assert persisted.webhook_delivered_at is None
    assert persisted.completed_attempt == 0
    resumed_webhook = AsyncMock()
    await asyncio.wait_for(
        ProcessingService(SQLPaymentStore(db), gateway, resumed_webhook).process(event), 5
    )

    finished = await repo.get(payment.id)
    assert finished.status is persisted.status
    assert finished.processed_at == persisted.processed_at
    assert finished.webhook_delivered_at is not None
    gateway.charge.assert_awaited_once()
    resumed_webhook.send.assert_awaited_once()


async def test_gateway_technical_error_retries_while_payment_is_pending(db, repo):
    payment, event = await payment_and_event(db, repo)
    gateway = AsyncMock()
    gateway.charge.side_effect = [RuntimeError("gateway offline"), PaymentStatus.SUCCEEDED]
    webhook = AsyncMock()
    service = ProcessingService(SQLPaymentStore(db), gateway, webhook)

    await service.process(event)

    persisted = await repo.get(payment.id)
    assert persisted.status is PaymentStatus.PENDING
    assert persisted.processed_at is None
    assert persisted.completed_attempt == 1
    assert persisted.last_error == "RuntimeError"
    webhook.send.assert_not_awaited()
    retry = (await delivery_records(db, payment.id))[-1]
    await service.process(PaymentEvent.model_validate(retry.payload))

    assert (await repo.get(payment.id)).status is PaymentStatus.SUCCEEDED
    assert gateway.charge.await_count == 2
    webhook.send.assert_awaited_once()


async def test_fully_delivered_payment_skips_even_later_attempts(db, repo):
    payment, event = await payment_and_event(db, repo)
    gateway = AsyncMock()
    gateway.charge.return_value = PaymentStatus.SUCCEEDED
    webhook = AsyncMock()
    service = ProcessingService(SQLPaymentStore(db), gateway, webhook)

    await service.process(event)
    completed = await repo.get(payment.id)
    await service.process(event.model_copy(update={"attempt": 3, "event_id": uuid4()}))

    assert await repo.get(payment.id) == completed
    gateway.charge.assert_awaited_once()
    webhook.send.assert_awaited_once()


async def test_unknown_generation_cannot_charge_pending_payment(db, repo):
    payment, event = await payment_and_event(db, repo)
    gateway = AsyncMock()
    webhook = AsyncMock()
    await ProcessingService(SQLPaymentStore(db), gateway, webhook).process(
        event.model_copy(update={"event_id": uuid4()})
    )
    gateway.charge.assert_not_awaited()
    webhook.send.assert_not_awaited()
    assert (await repo.get(payment.id)).status is PaymentStatus.PENDING


async def test_lost_lock_connection_does_not_reconnect_to_write_progress(db, repo):
    payment, event = await payment_and_event(db, repo)

    async def lose_database_connection(_):
        async with db.begin() as connection:
            lock_pid = await connection.scalar(
                text("SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted LIMIT 1")
            )
            assert lock_pid is not None
            await connection.execute(text("SELECT pg_terminate_backend(:pid)"), {"pid": lock_pid})
        return PaymentStatus.SUCCEEDED

    gateway = AsyncMock()
    gateway.charge.side_effect = lose_database_connection
    webhook = AsyncMock()
    with pytest.raises(ConnectionError, match="lock connection was lost"):
        await ProcessingService(SQLPaymentStore(db), gateway, webhook).process(event)
    persisted = await repo.get(payment.id)
    assert persisted.status is PaymentStatus.PENDING
    assert persisted.completed_attempt == 0
    assert len(await delivery_records(db, payment.id)) == 1
    webhook.send.assert_not_awaited()


async def test_manual_replay_preserves_result_and_rejects_old_cycle(db, repo, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", os.environ["INTEGRATION_DATABASE_URL"])
    payment, original = await payment_and_event(db, repo)
    gateway = AsyncMock()
    gateway.charge.return_value = PaymentStatus.SUCCEEDED
    webhook = AsyncMock()
    webhook.send.side_effect = WebhookDeliveryError("unavailable")
    service = ProcessingService(SQLPaymentStore(db), gateway, webhook)
    event = original
    for _ in range(3):
        await service.process(event)
        event = PaymentEvent.model_validate((await delivery_records(db, payment.id))[-1].payload)
    before = await repo.get(payment.id)
    async with async_sessionmaker(db)() as session, session.begin():
        await session.execute(update(Outbox).values(published_at=datetime.now(UTC)))
    await replay(payment.id)
    replay_event = PaymentEvent.model_validate((await delivery_records(db, payment.id))[-1].payload)
    assert replay_event.event_id != original.event_id
    await service.process(event)
    assert webhook.send.await_count == 3
    webhook.send.side_effect = None
    await service.process(replay_event)
    after = await repo.get(payment.id)
    assert after.status == before.status
    assert after.processed_at == before.processed_at
    assert after.webhook_delivered_at is not None
    assert after.completed_attempt == 1
    gateway.charge.assert_awaited_once()
    assert webhook.send.await_count == 4
