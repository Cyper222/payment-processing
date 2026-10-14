"""Payment state and transactional message delivery."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.String(1000), nullable=False),
        sa.Column("metadata", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("idempotency_key", sa.String(200), nullable=False),
        sa.Column("request_fingerprint", sa.String(64), nullable=False),
        sa.Column("processing_event_id", sa.Uuid(), nullable=False),
        sa.Column("webhook_url", sa.String(2048), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True)),
        sa.Column("completed_attempt", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.String(200)),
        sa.UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        sa.CheckConstraint("amount > 0", name="ck_payments_amount"),
        sa.CheckConstraint("currency IN ('RUB','USD','EUR')", name="ck_payments_currency"),
        sa.CheckConstraint("status IN ('pending','succeeded','failed')", name="ck_payments_status"),
        sa.CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status <> 'pending' AND processed_at IS NOT NULL)",
            name="ck_payments_processed_at",
        ),
        sa.CheckConstraint("completed_attempt BETWEEN 0 AND 3", name="ck_payments_attempt"),
        sa.CheckConstraint(
            "webhook_delivered_at IS NULL OR processed_at IS NOT NULL",
            name="ck_payments_webhook_completed",
        ),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("payment_id", sa.Uuid(), sa.ForeignKey("payments.id"), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("destination", sa.String(64), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("event_id", "attempt", "destination", name="uq_outbox_delivery"),
        sa.CheckConstraint("attempt BETWEEN 1 AND 3", name="ck_outbox_attempt"),
    )
    op.create_index(
        "ix_outbox_pending",
        "outbox",
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_table("outbox")
    op.drop_table("payments")
