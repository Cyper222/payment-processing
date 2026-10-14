import random
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from payment_service.domain import Currency, Payment, PaymentStatus
from payment_service.infrastructure.gateway import SimulatedGateway


@pytest.mark.parametrize(
    ("seed", "expected_status"),
    [(0, PaymentStatus.SUCCEEDED), (2, PaymentStatus.FAILED)],
)
async def test_simulator_has_deterministic_business_outcomes(seed, expected_status):
    delays = []

    async def record_sleep(delay):
        delays.append(delay)

    payment = Payment(
        id=uuid4(),
        amount=Decimal("12.50"),
        currency=Currency.RUB,
        description="test",
        metadata={},
        webhook_url="https://example.com/hook",
        status=PaymentStatus.PENDING,
        idempotency_key="test",
        created_at=datetime.now(UTC),
        processed_at=None,
    )
    gateway = SimulatedGateway(rng=random.Random(seed), sleep=record_sleep)

    assert await gateway.charge(payment) is expected_status
    assert delays == [random.Random(seed).uniform(2, 5)]
    assert 2 <= delays[0] <= 5
    assert payment.status is PaymentStatus.PENDING
