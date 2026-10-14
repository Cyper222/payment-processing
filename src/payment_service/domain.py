"""Payment values and errors, independent of transport and persistence."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class NewPayment:
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str


@dataclass(frozen=True, slots=True)
class Payment:
    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str
    status: PaymentStatus
    idempotency_key: str
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None = None
    completed_attempt: int = 0
    last_error: str | None = None
    processing_event_id: UUID | None = None


class PaymentEvent(BaseModel):
    """Stable event identity is retained through all automatic retries."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    event_id: UUID
    payment_id: UUID
    attempt: int = Field(default=1, ge=1, le=3, strict=True)
    error: str | None = Field(default=None, max_length=200)


class PaymentNotFound(Exception):
    """No payment exists for this identifier."""


class IdempotencyConflict(Exception):
    """An idempotency key was already used for a different request."""
