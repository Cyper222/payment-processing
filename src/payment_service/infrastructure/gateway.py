"""A side-effect-free payment gateway suitable for deterministic tests."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable

from payment_service.domain import Payment, PaymentStatus


class SimulatedGateway:
    """A declined charge is a business outcome, never a retryable exception."""

    def __init__(
        self,
        *,
        rng: random.Random | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._rng = rng if rng is not None else random.SystemRandom()
        self._sleep = sleep

    async def charge(self, payment: Payment) -> PaymentStatus:
        await self._sleep(self._rng.uniform(2.0, 5.0))
        return PaymentStatus.SUCCEEDED if self._rng.random() < 0.9 else PaymentStatus.FAILED
