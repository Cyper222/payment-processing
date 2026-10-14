"""Confirmed AMQP publishing and one explicitly acknowledged consumer."""

import asyncio
import base64
import logging
from collections.abc import Awaitable, Callable
from typing import Protocol
from uuid import uuid4

from faststream import AckPolicy
from faststream.rabbit import (
    Channel,
    ExchangeType,
    QueueType,
    RabbitBroker,
    RabbitExchange,
    RabbitMessage,
    RabbitQueue,
)
from faststream.rabbit.message import RabbitMessage as RawRabbitMessage
from pamqp.commands import Basic
from pydantic import JsonValue, ValidationError

from payment_service.domain import PaymentEvent

logger = logging.getLogger(__name__)

EVENTS_EXCHANGE = RabbitExchange("payments.events", type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)
MAIN_QUEUE = RabbitQueue(
    "payments.new",
    queue_type=QueueType.QUORUM,
    durable=True,
    arguments={
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
        "x-dead-letter-routing-key": "payments.dlq",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
        # Last-resort bound if neither PostgreSQL nor republishing is available.
        "x-delivery-limit": 3,
    },
)
RETRY_2S_QUEUE = RabbitQueue(
    "payments.retry.2s",
    queue_type=QueueType.QUORUM,
    durable=True,
    arguments={
        "x-message-ttl": 2000,
        "x-dead-letter-exchange": EVENTS_EXCHANGE.name,
        "x-dead-letter-routing-key": MAIN_QUEUE.name,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    },
)
RETRY_4S_QUEUE = RabbitQueue(
    "payments.retry.4s",
    queue_type=QueueType.QUORUM,
    durable=True,
    arguments={
        "x-message-ttl": 4000,
        "x-dead-letter-exchange": EVENTS_EXCHANGE.name,
        "x-dead-letter-routing-key": MAIN_QUEUE.name,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    },
)
DEAD_LETTER_QUEUE = RabbitQueue("payments.dlq", queue_type=QueueType.QUORUM, durable=True)
QUEUES = (MAIN_QUEUE, RETRY_2S_QUEUE, RETRY_4S_QUEUE, DEAD_LETTER_QUEUE)


class Publisher(Protocol):
    async def publish(
        self, payload: dict[str, JsonValue], destination: str, message_id: str
    ) -> None:
        """Return only once the broker confirms that a queue accepted the message."""
        ...


def build_broker(url: str) -> RabbitBroker:
    return RabbitBroker(
        url,
        default_channel=Channel(
            prefetch_count=1,
            publisher_confirms=True,
            on_return_raises=True,
        ),
        timeout=5,
        reconnect_interval=2,
        graceful_timeout=30,
        logger=logger,
    )


async def declare_topology(broker: RabbitBroker) -> None:
    """Declare all retry/DLQ targets before the consumer or dispatcher starts."""
    await broker.declare_exchange(EVENTS_EXCHANGE)
    await broker.declare_exchange(DEAD_LETTER_EXCHANGE)
    for definition in QUEUES:
        queue = await broker.declare_queue(definition)
        exchange = DEAD_LETTER_EXCHANGE if definition is DEAD_LETTER_QUEUE else EVENTS_EXCHANGE
        await queue.bind(exchange.name, routing_key=definition.name)


class RabbitPublisher:
    def __init__(self, broker: RabbitBroker, timeout: float = 5) -> None:
        self.broker = broker
        self.timeout = timeout

    async def publish(
        self, payload: dict[str, JsonValue], destination: str, message_id: str
    ) -> None:
        if destination not in {queue.name for queue in QUEUES}:
            raise ValueError("Unknown outbox destination")
        exchange = DEAD_LETTER_EXCHANGE if destination == "payments.dlq" else EVENTS_EXCHANGE
        async with asyncio.timeout(self.timeout):
            confirmation = await self.broker.publish(
                payload,
                exchange=exchange,
                routing_key=destination,
                persist=True,
                mandatory=True,
                timeout=self.timeout,
                message_id=message_id,
                correlation_id=str(payload.get("event_id", message_id)),
            )
        if not isinstance(confirmation, Basic.Ack):
            raise RuntimeError("RabbitMQ did not confirm publication")


async def raw_decoder(message: RawRabbitMessage) -> bytes:
    # Decode/validate inside the handler, so invalid JSON also follows its DLQ/ACK rules.
    return message.body


class PaymentMessageHandler:
    def __init__(
        self, process: Callable[[PaymentEvent], Awaitable[None]], publisher: Publisher
    ) -> None:
        self.process = process
        self.publisher = publisher

    async def __call__(self, body: bytes, message: RawRabbitMessage) -> None:
        try:
            event = PaymentEvent.model_validate_json(body)
        except ValidationError:
            # Preserve bytes for offline diagnosis, without logging untrusted payloads.
            payload: dict[str, JsonValue] = {
                "error": "invalid_payment_event",
                "original_body_base64": base64.b64encode(body).decode("ascii"),
            }
            await self._transfer(message, payload, "payments.dlq", str(uuid4()))
            return

        context = {
            "payment_id": str(event.payment_id),
            "event_id": str(event.event_id),
            "attempt": event.attempt,
        }
        logger.info("payment_message_received", extra=context)
        try:
            await self.process(event)
        except Exception as error:
            # When a DB transaction cannot commit, no transactional retry can exist.
            # Confirm the replacement before ACK; duplicates remain safe in the service.
            reason = type(error).__name__[:200]
            logger.error("payment_processing_unavailable", extra={**context, "error": reason})
            if event.attempt < 3:
                destination = f"payments.retry.{2**event.attempt}s"
                replacement = event.model_copy(
                    update={"attempt": event.attempt + 1, "error": reason}
                )
            else:
                destination = "payments.dlq"
                replacement = event.model_copy(update={"error": reason})
            await self._transfer(
                message,
                replacement.model_dump(mode="json"),
                destination,
                str(event.event_id),
            )
        else:
            # process() returns only after completion or durable retry/DLQ scheduling.
            await message.ack()

    async def _transfer(
        self,
        message: RawRabbitMessage,
        payload: dict[str, JsonValue],
        destination: str,
        message_id: str,
    ) -> None:
        try:
            await self.publisher.publish(payload, destination, message_id)
        except Exception as error:
            logger.error(
                "message_transfer_failed",
                extra={"event_id": message_id, "error": type(error).__name__},
            )
            # Main queue's quorum delivery limit prevents endless poison redeliveries;
            # at-least-once dead-lettering retains the original until DLQ confirms it.
            await message.nack(requeue=True)
        else:
            await message.ack()


def register_consumer(
    broker: RabbitBroker,
    process: Callable[[PaymentEvent], Awaitable[None]],
    publisher: Publisher,
) -> PaymentMessageHandler:
    handler = PaymentMessageHandler(process, publisher)

    @broker.subscriber(
        MAIN_QUEUE,
        EVENTS_EXCHANGE,
        ack_policy=AckPolicy.MANUAL,
        decoder=raw_decoder,
        no_reply=True,
    )
    async def consume(body: bytes, message: RabbitMessage) -> None:
        await handler(body, message)

    return handler
