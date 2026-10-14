"""Transactional outbox relay with concurrent dispatcher support."""

import asyncio
import logging
import time
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.infrastructure.messaging import Publisher
from payment_service.infrastructure.models import Outbox

logger = logging.getLogger(__name__)


class OutboxDispatcher:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        publisher: Publisher,
        poll_interval: float = 0.5,
    ) -> None:
        self.sessions = sessions
        self.publisher = publisher
        self.poll_interval = poll_interval
        self.last_iteration_at = time.monotonic()

    async def dispatch_once(self) -> bool:
        """Publish one locked row; rollback keeps it pending on any failure.

        Publishing while holding a short row lock lets multiple dispatchers coexist.
        A crash after the confirm but before COMMIT deliberately causes a duplicate.
        """
        async with self.sessions() as session, session.begin():
            event = await session.scalar(
                select(Outbox)
                .where(Outbox.published_at.is_(None))
                .order_by(Outbox.created_at, Outbox.id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if event is None:
                return False
            await self.publisher.publish(event.payload, event.destination, str(event.id))
            event.published_at = datetime.now(UTC)
            context = {
                "payment_id": str(event.payment_id),
                "event_id": str(event.event_id),
                "attempt": event.attempt,
                "destination": event.destination,
            }
        logger.info("outbox_published", extra=context)
        return True

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                published = await self.dispatch_once()
            except Exception as error:
                logger.error("outbox_publication_failed", extra={"error": type(error).__name__})
                published = False
            finally:
                self.last_iteration_at = time.monotonic()
            if not published:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.poll_interval)
                except TimeoutError:
                    pass
