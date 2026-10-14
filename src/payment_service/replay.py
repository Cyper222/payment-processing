"""Explicit operator recovery: preserve the payment result and schedule a new delivery cycle."""

import argparse
import asyncio
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from payment_service.config import Settings
from payment_service.domain import PaymentEvent
from payment_service.infrastructure.database import SQLPaymentStore, create_engine, outbox_record
from payment_service.infrastructure.models import Outbox, PaymentRow


async def replay(payment_id: UUID) -> None:
    engine = create_engine(Settings())
    try:
        async with SQLPaymentStore(engine).locked(payment_id) as repository:
            payment = await repository.get(payment_id)
            if payment.webhook_delivered_at:
                raise ValueError("Webhook is already delivered; replay is unnecessary")
            event_id = uuid4()
            async with async_sessionmaker(engine)() as session, session.begin():
                pending = await session.scalar(
                    select(Outbox.id)
                    .where(Outbox.payment_id == payment_id, Outbox.published_at.is_(None))
                    .limit(1)
                )
                if pending:
                    raise ValueError("Unpublished outbox records remain; restore dispatcher first")
                await session.execute(
                    update(PaymentRow)
                    .where(PaymentRow.id == payment_id)
                    .values(
                        completed_attempt=0,
                        last_error=None,
                        processing_event_id=event_id,
                    )
                )
                session.add(
                    outbox_record(
                        PaymentEvent(event_id=event_id, payment_id=payment_id), "payments.new"
                    )
                )
        print(f"Replay scheduled for {payment_id}")
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("payment_id", type=UUID)
    args = parser.parse_args()
    asyncio.run(replay(args.payment_id))


if __name__ == "__main__":
    main()
