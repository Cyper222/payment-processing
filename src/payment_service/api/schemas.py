"""Public API schemas and exact decimal amount constraints."""

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

from payment_service.domain import Currency, NewPayment, PaymentStatus


class CreatePayment(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Currency
    description: str = Field(default="", max_length=1000)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: str = Field(min_length=1, max_length=2048)

    @field_validator("amount", mode="before")
    @classmethod
    def reject_boolean_amount(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("Amount must be a decimal")
        return value

    @model_validator(mode="after")
    def finite_metadata(self) -> Self:
        import json

        try:
            encoded = json.dumps(self.metadata, allow_nan=False)
        except ValueError as exc:
            raise ValueError("Metadata must contain finite JSON values") from exc
        if len(encoded.encode()) > 16_384:
            raise ValueError("Metadata exceeds 16 KiB")
        return self

    def command(self, normalized_url: str) -> NewPayment:
        return NewPayment(
            amount=self.amount.quantize(Decimal("0.01")),
            currency=self.currency,
            description=self.description,
            metadata=self.metadata,
            webhook_url=normalized_url,
        )


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetails(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None
    completed_attempt: int
    last_error: str | None
    processing_event_id: UUID
