"""Real RabbitMQ confirms, manual ACK, TTL routing, and bounded poison delivery."""

import asyncio
import base64
import json
import os
import time
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
import pytest_asyncio
from aio_pika import IncomingMessage, RobustQueue
from aio_pika.exceptions import DeliveryError
from faststream.rabbit import RabbitBroker

from payment_service.domain import PaymentEvent
from payment_service.infrastructure.messaging import (
    DEAD_LETTER_QUEUE,
    EVENTS_EXCHANGE,
    MAIN_QUEUE,
    QUEUES,
    RabbitPublisher,
    build_broker,
    declare_topology,
    register_consumer,
)

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def broker() -> AsyncIterator[RabbitBroker]:
    url = os.environ.get("INTEGRATION_RABBITMQ_URL")
    if not url:
        pytest.skip("INTEGRATION_RABBITMQ_URL is not configured")
    connection = build_broker(url)
    await connection.connect()
    await declare_topology(connection)
    for definition in QUEUES:
        queue = await connection.declare_queue(definition)
        await queue.purge()
    try:
        yield connection
    finally:
        await connection.stop()


async def receive(queue: RobustQueue) -> IncomingMessage:
    async with asyncio.timeout(12):
        while True:
            result = await queue.get(fail=False)
            if result is not None:
                await result.ack()
                return result
            await asyncio.sleep(0.05)


async def test_confirmed_persistent_publication_reaches_queue(broker: RabbitBroker) -> None:
    event = PaymentEvent(event_id=uuid4(), payment_id=uuid4())
    await RabbitPublisher(broker).publish(event.model_dump(mode="json"), "payments.new", "test-id")
    message = await receive(await broker.declare_queue(MAIN_QUEUE))
    assert PaymentEvent.model_validate_json(message.body) == event
    assert message.delivery_mode == 2
    assert message.message_id == "test-id"


async def test_unroutable_message_is_not_accepted_as_confirmed(broker: RabbitBroker) -> None:
    queue = await broker.declare_queue(MAIN_QUEUE)
    await queue.unbind(EVENTS_EXCHANGE.name, routing_key=MAIN_QUEUE.name)
    try:
        with pytest.raises(DeliveryError):
            await RabbitPublisher(broker).publish({}, "payments.new", "unroutable-id")
    finally:
        await queue.bind(EVENTS_EXCHANGE.name, routing_key=MAIN_QUEUE.name)


async def test_single_handler_acknowledges_success(broker: RabbitBroker) -> None:
    processed = asyncio.Event()
    event = PaymentEvent(event_id=uuid4(), payment_id=uuid4())

    async def process(received: PaymentEvent) -> None:
        assert received == event
        processed.set()

    publisher = RabbitPublisher(broker)
    register_consumer(broker, process, publisher)
    await broker.start()
    await publisher.publish(event.model_dump(mode="json"), "payments.new", "success-id")
    await asyncio.wait_for(processed.wait(), timeout=5)
    await broker.stop()

    # Closing the channel would requeue an unacknowledged delivery.
    inspector = build_broker(os.environ["INTEGRATION_RABBITMQ_URL"])
    await inspector.connect()
    try:
        queue = await inspector.declare_queue(MAIN_QUEUE)
        assert await queue.get(fail=False) is None
    finally:
        await inspector.stop()


async def test_three_attempts_backoff_and_diagnostic_dlq(broker: RabbitBroker) -> None:
    attempts: list[tuple[int, float]] = []

    async def unavailable(event: PaymentEvent) -> None:
        attempts.append((event.attempt, time.monotonic()))
        raise ConnectionError("do not disclose credentials from exceptions")

    publisher = RabbitPublisher(broker)
    register_consumer(broker, unavailable, publisher)
    await broker.start()
    event = PaymentEvent(event_id=uuid4(), payment_id=uuid4())
    await publisher.publish(event.model_dump(mode="json"), "payments.new", "retry-id")
    message = await receive(await broker.declare_queue(DEAD_LETTER_QUEUE))
    final = PaymentEvent.model_validate_json(message.body)
    assert final.event_id == event.event_id
    assert final.attempt == 3
    assert final.error == "ConnectionError"
    assert [item[0] for item in attempts] == [1, 2, 3]
    assert attempts[1][1] - attempts[0][1] >= 1.9
    assert attempts[2][1] - attempts[1][1] >= 3.9


async def test_malformed_json_is_diagnosed_and_dead_lettered(broker: RabbitBroker) -> None:
    process = AsyncMock()
    register_consumer(broker, process, RabbitPublisher(broker))
    await broker.start()
    body = b'{"unclosed": '
    await broker.publish(
        body,
        exchange=EVENTS_EXCHANGE,
        routing_key=MAIN_QUEUE.name,
        persist=True,
        content_type="application/json",
    )
    message = await receive(await broker.declare_queue(DEAD_LETTER_QUEUE))
    payload = json.loads(message.body)
    assert payload["error"] == "invalid_payment_event"
    assert base64.b64decode(payload["original_body_base64"]) == body
    process.assert_not_awaited()


async def test_quorum_delivery_limit_stops_failed_republishing(broker: RabbitBroker) -> None:
    process = AsyncMock(side_effect=ConnectionError)
    broken_publisher = Mock(publish=AsyncMock(side_effect=TimeoutError))
    register_consumer(broker, process, broken_publisher)
    await broker.start()
    event = PaymentEvent(event_id=uuid4(), payment_id=uuid4())
    await RabbitPublisher(broker).publish(
        event.model_dump(mode="json"), "payments.new", "poison-id"
    )
    message = await receive(await broker.declare_queue(DEAD_LETTER_QUEUE))
    assert PaymentEvent.model_validate_json(message.body) == event
    assert 1 <= process.await_count <= 4  # initial delivery + bounded broker redeliveries
    assert message.headers["x-first-death-reason"] == "delivery_limit"
    assert message.headers["x-first-death-queue"] == "payments.new"
