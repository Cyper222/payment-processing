"""Exercise the running Compose demo: auth, idempotency, webhook, retries and DLQ."""

import asyncio
import json
import os
import time
from uuid import uuid4

import httpx
from dotenv import load_dotenv


async def main() -> None:
    load_dotenv()
    api_url = f"http://127.0.0.1:{os.getenv('API_PORT', '8000')}"
    receiver_url = f"http://127.0.0.1:{os.getenv('WEBHOOK_DEMO_PORT', '8081')}"
    management_url = f"http://127.0.0.1:{os.getenv('RABBITMQ_MANAGEMENT_PORT', '15672')}"
    async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
        assert (await client.get(f"{api_url}/health")).status_code == 401
        headers = {"X-API-Key": os.environ["API_KEY"]}
        assert (await client.get(f"{api_url}/health", headers=headers)).status_code == 200
        payments: dict[str, tuple[str, int]] = {}
        for label, failures in (("success", 0), ("retry", 2), ("dlq", 100)):
            request_headers = {**headers, "Idempotency-Key": f"smoke-{label}-{uuid4()}"}
            payload = {
                "amount": "1500.00",
                "currency": "RUB",
                "description": f"Smoke {label}",
                "metadata": {"smoke": True},
                "webhook_url": f"http://webhook-receiver:8080/events?fail_first={failures}",
            }
            response = await client.post(
                f"{api_url}/api/v1/payments", headers=request_headers, json=payload
            )
            assert response.status_code == 202, response.text
            payment_id = response.json()["payment_id"]
            duplicate = await client.post(
                f"{api_url}/api/v1/payments", headers=request_headers, json=payload
            )
            assert duplicate.status_code == 202
            assert duplicate.json()["payment_id"] == payment_id
            conflict = await client.post(
                f"{api_url}/api/v1/payments",
                headers=request_headers,
                json={**payload, "amount": "1.00"},
            )
            assert conflict.status_code == 409
            payments[label] = (payment_id, failures)

        deadline = time.monotonic() + 90
        details: dict[str, dict[str, object]] = {}
        while time.monotonic() < deadline:
            for label, (payment_id, _) in payments.items():
                response = await client.get(
                    f"{api_url}/api/v1/payments/{payment_id}", headers=headers
                )
                response.raise_for_status()
                details[label] = response.json()
            if (
                details["success"]["webhook_delivered_at"]
                and details["retry"]["webhook_delivered_at"]
                and details["dlq"]["completed_attempt"] == 3
            ):
                break
            await asyncio.sleep(0.5)
        else:
            raise AssertionError(f"Payment processing did not finish: {details}")

        events = (await client.get(f"{receiver_url}/events")).json()["events"]
        for label, (payment_id, _) in payments.items():
            attempts = [event for event in events if event["event_id"] == payment_id]
            expected = {"success": [200], "retry": [503, 503, 200], "dlq": [503, 503, 503]}
            assert [event["status_code"] for event in attempts] == expected[label]
            assert all(event["payload"] == attempts[0]["payload"] for event in attempts)
            assert details[label]["status"] in {"succeeded", "failed"}
        assert not details["dlq"]["webhook_delivered_at"]

        # Management get with requeue preserves DLQ evidence for operator inspection.
        auth = (os.getenv("RABBITMQ_USER", "payments"), os.environ["RABBITMQ_PASSWORD"])
        for _ in range(20):
            response = await client.post(
                f"{management_url}/api/queues/%2F/payments.dlq/get",
                auth=auth,
                json={"count": 1000, "ackmode": "ack_requeue_true", "encoding": "auto"},
            )
            response.raise_for_status()
            if any(
                json.loads(message["payload"]).get("payment_id") == payments["dlq"][0]
                for message in response.json()
            ):
                break
            await asyncio.sleep(0.5)
        else:
            raise AssertionError("Exhausted payment was not found in DLQ")
        print(
            json.dumps(
                {
                    "result": "passed",
                    "scenarios": ["auth", "idempotency", "webhook", "retry", "dlq"],
                    "payments": {label: value[0] for label, value in payments.items()},
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
