"""Consumer + outbox relay process with signal handling and readiness heartbeat."""

import argparse
import asyncio
import logging
import signal
import time
from pathlib import Path

from faststream.rabbit import RabbitBroker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from payment_service.application.processing import ProcessingService
from payment_service.config import Settings
from payment_service.infrastructure.database import SQLPaymentStore, create_engine
from payment_service.infrastructure.gateway import SimulatedGateway
from payment_service.infrastructure.logging import configure_logging
from payment_service.infrastructure.messaging import (
    RabbitPublisher,
    build_broker,
    declare_topology,
    register_consumer,
)
from payment_service.infrastructure.outbox import OutboxDispatcher
from payment_service.infrastructure.webhook import HttpWebhookSender

logger = logging.getLogger(__name__)
HEARTBEAT = Path("/tmp/payment-worker.heartbeat")


def is_healthy() -> bool:
    try:
        return 0 <= time.time() - float(HEARTBEAT.read_text()) < 10
    except (OSError, ValueError):
        return False


async def heartbeat(
    engine: AsyncEngine,
    broker: RabbitBroker,
    dispatcher: OutboxDispatcher,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        try:
            async with asyncio.timeout(3):
                async with engine.connect() as connection:
                    await connection.execute(text("SELECT 1"))
                broker_ready = await broker.ping(timeout=1)
            relay_alive = time.monotonic() - dispatcher.last_iteration_at < 15
            if broker_ready and relay_alive:
                await asyncio.to_thread(HEARTBEAT.write_text, str(time.time()))
            else:
                await asyncio.to_thread(HEARTBEAT.unlink, missing_ok=True)
        except Exception as error:
            await asyncio.to_thread(HEARTBEAT.unlink, missing_ok=True)
            logger.warning("worker_not_ready", extra={"error": type(error).__name__})
        try:
            await asyncio.wait_for(stop.wait(), timeout=2)
        except TimeoutError:
            pass


async def run_worker() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    engine = create_engine(settings)
    broker = build_broker(settings.rabbitmq_url)
    publisher = RabbitPublisher(broker, settings.publish_timeout)
    dispatcher = OutboxDispatcher(
        async_sessionmaker(engine, expire_on_commit=False), publisher, settings.outbox_poll_interval
    )
    webhook = HttpWebhookSender(
        timeout=settings.webhook_timeout,
        allowed_hosts=settings.webhook_allowed_hosts,
        signing_secret=(
            settings.webhook_signing_secret.get_secret_value()
            if settings.webhook_signing_secret
            else None
        ),
    )
    service = ProcessingService(SQLPaymentStore(engine), SimulatedGateway(), webhook)
    register_consumer(broker, service.process, publisher)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)
    await asyncio.to_thread(HEARTBEAT.unlink, missing_ok=True)
    tasks: list[asyncio.Task[object]] = []
    try:
        async with asyncio.timeout(10):
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
            await broker.connect()
            await declare_topology(broker)
            await broker.start()
        tasks = [
            asyncio.create_task(dispatcher.run(stop), name="outbox-dispatcher"),
            asyncio.create_task(heartbeat(engine, broker, dispatcher, stop), name="readiness"),
            asyncio.create_task(stop.wait(), name="shutdown"),
        ]
        logger.info("worker_started")
        # An unexpected background task exit must fail the process for Compose to restart it.
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
        if not stop.is_set():
            raise RuntimeError("Worker background task stopped unexpectedly")
    finally:
        await asyncio.to_thread(HEARTBEAT.unlink, missing_ok=True)
        stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # FastStream cancels intake, waits for active handlers, and closes AMQP last.
        # A cancelled relay transaction rolls back; confirmed events may be redelivered.
        try:
            await broker.stop()
        finally:
            await engine.dispose()
            for signum in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(signum)
        logger.info("worker_stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--healthcheck", action="store_true")
    args = parser.parse_args()
    if args.healthcheck:
        raise SystemExit(0 if is_healthy() else 1)
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
