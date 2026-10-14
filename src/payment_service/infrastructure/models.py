"""SQLAlchemy mappings; schema creation belongs to Alembic."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class PaymentRow(Base):
    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        CheckConstraint("amount > 0", name="ck_payments_amount"),
        CheckConstraint("currency IN ('RUB','USD','EUR')", name="ck_payments_currency"),
        CheckConstraint("status IN ('pending','succeeded','failed')", name="ck_payments_status"),
        CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status <> 'pending' AND processed_at IS NOT NULL)",
            name="ck_payments_processed_at",
        ),
        CheckConstraint("completed_attempt BETWEEN 0 AND 3", name="ck_payments_attempt"),
        CheckConstraint(
            "webhook_delivered_at IS NULL OR processed_at IS NOT NULL",
            name="ck_payments_webhook_completed",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(1000))
    metadata_json: Mapped[dict[str, JsonValue]] = mapped_column("metadata", JSONB)
    status: Mapped[str] = mapped_column(String(16), server_default="pending")
    idempotency_key: Mapped[str] = mapped_column(String(200))
    request_fingerprint: Mapped[str] = mapped_column(String(64))
    processing_event_id: Mapped[UUID]
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_attempt: Mapped[int] = mapped_column(Integer, server_default="0")
    last_error: Mapped[str | None] = mapped_column(String(200))


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        UniqueConstraint("event_id", "attempt", "destination", name="uq_outbox_delivery"),
        CheckConstraint("attempt BETWEEN 1 AND 3", name="ck_outbox_attempt"),
        Index("ix_outbox_pending", "created_at", postgresql_where=text("published_at IS NULL")),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"))
    event_id: Mapped[UUID]
    attempt: Mapped[int] = mapped_column(Integer)
    destination: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
