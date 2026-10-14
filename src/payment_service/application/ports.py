"""Contracts used by payment orchestration and test doubles."""

from contextlib import AbstractAsyncContextManager
from typing import Protocol
from uuid import UUID

from payment_service.domain import Payment, PaymentEvent, PaymentStatus


class Gateway(Protocol):
    async def charge(self, payment: Payment) -> PaymentStatus: ...


class WebhookSender(Protocol):
    async def send(self, payment: Payment) -> None: ...


class ProcessingRepository(Protocol):
    async def get(self, payment_id: UUID) -> Payment: ...

    async def save_result(self, payment_id: UUID, status: PaymentStatus) -> Payment: ...

    async def mark_delivered(self, event: PaymentEvent) -> None: ...

    async def schedule_failure(self, event: PaymentEvent, error: str) -> None: ...


class ProcessingStore(Protocol):
    def locked(self, payment_id: UUID) -> AbstractAsyncContextManager[ProcessingRepository]: ...
