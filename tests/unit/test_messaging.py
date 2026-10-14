import asyncio
import base64
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from pamqp.commands import Basic

from payment_service.domain import PaymentEvent
from payment_service.infrastructure.messaging import (
    DEAD_LETTER_EXCHANGE,
    EVENTS_EXCHANGE,
    MAIN_QUEUE,
    QUEUES,
    RETRY_2S_QUEUE,
    RETRY_4S_QUEUE,
    PaymentMessageHandler,
    RabbitPublisher,
    declare_topology,
)


@pytest.fixture
def event() -> PaymentEvent:
    return PaymentEvent(event_id=uuid4(), payment_id=uuid4())


@pytest.fixture
def message() -> Mock:
    return Mock(ack=AsyncMock(), nack=AsyncMock())


async def test_ack_only_after_processing_completes(event: PaymentEvent, message: Mock) -> None:
    async def process(received: PaymentEvent) -> None:
        assert received == event
        message.ack.assert_not_awaited()

    publisher = Mock(publish=AsyncMock())
    await PaymentMessageHandler(process, publisher)(event.model_dump_json().encode(), message)
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()
    publisher.publish.assert_not_awaited()


@pytest.mark.parametrize(
    ("attempt", "destination", "next_attempt"),
    [(1, "payments.retry.2s", 2), (2, "payments.retry.4s", 3), (3, "payments.dlq", 3)],
)
async def test_uncommitted_failure_is_confirmed_before_ack(
    event: PaymentEvent, message: Mock, attempt: int, destination: str, next_attempt: int
) -> None:
    event = event.model_copy(update={"attempt": attempt})
    process = AsyncMock(side_effect=ConnectionError("private database DSN must not be included"))

    async def publish(payload: dict, route: str, message_id: str) -> None:
        message.ack.assert_not_awaited()
        assert payload == event.model_copy(
            update={"attempt": next_attempt, "error": "ConnectionError"}
        ).model_dump(mode="json")
        assert route == destination
        assert message_id == str(event.event_id)

    publisher = Mock(publish=AsyncMock(side_effect=publish))
    await PaymentMessageHandler(process, publisher)(event.model_dump_json().encode(), message)
    publisher.publish.assert_awaited_once()
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()


async def test_failed_republish_keeps_original_unacked(event: PaymentEvent, message: Mock) -> None:
    process = AsyncMock(side_effect=ConnectionError)
    publisher = Mock(publish=AsyncMock(side_effect=TimeoutError))
    await PaymentMessageHandler(process, publisher)(event.model_dump_json().encode(), message)
    message.ack.assert_not_awaited()
    message.nack.assert_awaited_once_with(requeue=True)


@pytest.mark.parametrize("body", [b"{broken", b"null", b"{}", b'{"attempt":4}'])
async def test_malformed_messages_reach_dlq_before_ack(body: bytes, message: Mock) -> None:
    process = AsyncMock()

    async def publish(payload: dict, route: str, message_id: str) -> None:
        message.ack.assert_not_awaited()
        assert route == "payments.dlq"
        assert payload["error"] == "invalid_payment_event"
        assert base64.b64decode(payload["original_body_base64"]) == body

    publisher = Mock(publish=AsyncMock(side_effect=publish))
    await PaymentMessageHandler(process, publisher)(body, message)
    publisher.publish.assert_awaited_once()
    process.assert_not_awaited()
    message.ack.assert_awaited_once()


async def test_cancellation_does_not_ack_or_create_new_retry(
    event: PaymentEvent, message: Mock
) -> None:
    process = AsyncMock(side_effect=asyncio.CancelledError)
    publisher = Mock(publish=AsyncMock())
    with pytest.raises(asyncio.CancelledError):
        await PaymentMessageHandler(process, publisher)(event.model_dump_json().encode(), message)
    publisher.publish.assert_not_awaited()
    message.ack.assert_not_awaited()
    message.nack.assert_not_awaited()


async def test_publisher_requires_ack_frame_and_routes_persistently(event: PaymentEvent) -> None:
    broker = Mock(publish=AsyncMock(return_value=Basic.Ack()))
    publisher = RabbitPublisher(broker)
    await publisher.publish(event.model_dump(mode="json"), "payments.new", "outbox-id")
    kwargs = broker.publish.call_args.kwargs
    assert kwargs["exchange"] == EVENTS_EXCHANGE
    assert kwargs["routing_key"] == "payments.new"
    assert kwargs["mandatory"] is True
    assert kwargs["persist"] is True
    assert kwargs["message_id"] == "outbox-id"

    await publisher.publish(event.model_dump(mode="json"), "payments.dlq", "outbox-id")
    assert broker.publish.call_args.kwargs["exchange"] == DEAD_LETTER_EXCHANGE


@pytest.mark.parametrize("confirmation", [None, Basic.Nack(), Basic.Reject()])
async def test_publisher_refuses_missing_or_negative_confirmation(confirmation: object) -> None:
    publisher = RabbitPublisher(Mock(publish=AsyncMock(return_value=confirmation)))
    with pytest.raises(RuntimeError, match="did not confirm"):
        await publisher.publish({}, "payments.new", "event-id")


async def test_publisher_reconnect_wait_is_bounded() -> None:
    async def never_returns(*args: object, **kwargs: object) -> None:
        await asyncio.Event().wait()

    publisher = RabbitPublisher(Mock(publish=never_returns), timeout=0.01)
    with pytest.raises(TimeoutError):
        await publisher.publish({}, "payments.new", "event-id")


async def test_topology_declares_and_binds_every_queue_before_consume() -> None:
    queue = Mock(bind=AsyncMock())
    broker = Mock(declare_exchange=AsyncMock(), declare_queue=AsyncMock(return_value=queue))
    await declare_topology(broker)
    assert broker.declare_exchange.await_count == 2
    assert broker.declare_queue.await_count == len(QUEUES)
    assert queue.bind.await_count == len(QUEUES)
    assert all(item.durable for item in QUEUES)
    assert MAIN_QUEUE.arguments["x-delivery-limit"] == 3
    for retry, ttl in [(RETRY_2S_QUEUE, 2000), (RETRY_4S_QUEUE, 4000)]:
        assert retry.arguments["x-message-ttl"] == ttl
        assert retry.arguments["x-dead-letter-routing-key"] == "payments.new"
        assert retry.arguments["x-dead-letter-strategy"] == "at-least-once"
        assert retry.arguments["x-overflow"] == "reject-publish"
