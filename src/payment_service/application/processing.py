"""One payment orchestration, with persistent progress between deliveries."""

import logging

from payment_service.application.ports import Gateway, ProcessingStore, WebhookSender
from payment_service.domain import PaymentEvent, PaymentStatus

logger = logging.getLogger(__name__)


class ProcessingService:
    def __init__(self, store: ProcessingStore, gateway: Gateway, webhook: WebhookSender) -> None:
        self.store = store
        self.gateway = gateway
        self.webhook = webhook

    async def process(self, event: PaymentEvent) -> None:
        context = {
            "payment_id": str(event.payment_id),
            "event_id": str(event.event_id),
            "attempt": event.attempt,
        }
        async with self.store.locked(event.payment_id) as repository:
            payment = await repository.get(event.payment_id)
            if payment.processing_event_id and event.event_id != payment.processing_event_id:
                logger.info("obsolete_cycle_skipped", extra=context)
                return
            if payment.webhook_delivered_at or event.attempt <= payment.completed_attempt:
                logger.info("duplicate_skipped", extra=context)
                return
            try:
                if payment.status == PaymentStatus.PENDING:
                    result = await self.gateway.charge(payment)
                    if result == PaymentStatus.PENDING:
                        raise ValueError("Gateway must return a terminal result")
                    payment = await repository.save_result(payment.id, result)
                    logger.info("payment_processed", extra={**context, "status": result.value})
                await self.webhook.send(payment)
                await repository.mark_delivered(event)
                logger.info("webhook_delivered", extra=context)
            except Exception as exc:
                # Persist the retry decision before the handler acknowledges the message.
                # Exception messages can contain URLs/secrets: record only their class.
                error = type(exc).__name__
                await repository.schedule_failure(event, error)
                logger.warning("attempt_failed", extra={**context, "error": error})
