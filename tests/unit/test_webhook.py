import asyncio
import hashlib
import hmac
import json
import socket
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import httpx
import pytest

from payment_service.domain import Currency, Payment, PaymentStatus
from payment_service.infrastructure.webhook import (
    HttpWebhookSender,
    WebhookDeliveryError,
    validate_webhook_url,
)


@pytest.fixture
def payment():
    return Payment(
        id=UUID("d379ecca-ec8c-479d-99d1-6d08f3fd97af"),
        amount=Decimal("1500.00"),
        currency=Currency.RUB,
        description="test",
        metadata={},
        webhook_url="https://merchant.example:8443/webhook?token=private",
        status=PaymentStatus.SUCCEEDED,
        idempotency_key="not-an-outbound-credential",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        processed_at=datetime(2026, 1, 1, 0, 0, 3, tzinfo=UTC),
    )


async def public_dns(host, port):
    return ["8.8.8.8"]


@pytest.mark.parametrize("status_code", [200, 201, 202, 204, 299])
async def test_success_is_any_2xx_and_delivery_is_dns_pinned(payment, status_code):
    requests = []
    resolutions = []

    async def resolver(host, port):
        resolutions.append((host, port))
        return ["8.8.8.8"]

    def handler(request):
        requests.append(request)
        return httpx.Response(status_code)

    sender = HttpWebhookSender(
        resolver=resolver,
        transport_factory=lambda: httpx.MockTransport(handler),
    )
    await sender.send(payment)

    assert resolutions == [("merchant.example", 8443)]
    assert len(requests) == 1
    request = requests[0]
    assert str(request.url) == "https://8.8.8.8:8443/webhook?token=private"
    assert request.headers["host"] == "merchant.example:8443"
    assert request.extensions["sni_hostname"] == "merchant.example"
    assert request.headers["Idempotency-Key"] == str(payment.id)
    assert request.headers["X-Webhook-Id"] == str(payment.id)
    assert "X-API-Key" not in request.headers
    payload = json.loads(request.content)
    assert payload == {
        "event": "payment.completed",
        "notification_id": str(payment.id),
        "payment_id": str(payment.id),
        "status": "succeeded",
        "amount": "1500.00",
        "currency": "RUB",
        "processed_at": "2026-01-01T00:00:03+00:00",
    }


async def test_retry_payload_identity_and_hmac_are_stable(payment):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(204)

    sender = HttpWebhookSender(
        signing_secret="a-webhook-secret",
        resolver=public_dns,
        transport_factory=lambda: httpx.MockTransport(handler),
    )
    await sender.send(payment)
    await sender.send(payment)

    assert requests[0].content == requests[1].content
    assert requests[0].headers == requests[1].headers
    expected = hmac.new(b"a-webhook-secret", requests[0].content, hashlib.sha256).hexdigest()
    assert requests[0].headers["X-Webhook-Signature"] == f"sha256={expected}"


@pytest.mark.parametrize("status_code", [301, 400, 404, 429, 500, 503])
async def test_non_2xx_is_one_failed_attempt_without_following_redirects(payment, status_code):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            status_code,
            headers={"Location": "http://127.0.0.1/private"},
            text="receiver-secret",
        )

    sender = HttpWebhookSender(
        resolver=public_dns,
        transport_factory=lambda: httpx.MockTransport(handler),
    )
    with pytest.raises(WebhookDeliveryError, match=f"webhook returned HTTP {status_code}"):
        await sender.send(payment)
    assert len(requests) == 1
    assert payment.status is PaymentStatus.SUCCEEDED


async def test_response_body_is_closed_without_being_read(payment):
    class UntrustedBody(httpx.AsyncByteStream):
        closed = False

        async def __aiter__(self):
            raise AssertionError("must not read receiver body")
            yield b"unreachable"

        async def aclose(self):
            self.closed = True

    body = UntrustedBody()
    sender = HttpWebhookSender(
        resolver=public_dns,
        transport_factory=lambda: httpx.MockTransport(
            lambda request: httpx.Response(204, stream=body)
        ),
    )
    await sender.send(payment)
    assert body.closed


@pytest.mark.parametrize(
    "value",
    [
        "not-a-url",
        "/relative",
        "ftp://merchant.example/hook",
        "https://user:secret@merchant.example/hook",
        "https://@merchant.example/hook",
        "https://merchant.example/hook#fragment",
        "https://merchant.example/hook#",
        "http://localhost/hook",
        "http://child.localhost/hook",
        "http://127.0.0.1/hook",
        "http://169.254.169.254/metadata",
        "http://10.0.0.1/hook",
        "http://172.16.0.1/hook",
        "http://192.168.1.1/hook",
        "http://0.0.0.0/hook",
        "http://224.0.0.1/hook",
        "http://[::1]/hook",
        "http://[fe80::1]/hook",
        "http://[::ffff:127.0.0.1]/hook",
        "http://[::127.0.0.1]/hook",
        "http://[64:ff9b::127.0.0.1]/hook",
        "http://[2606:4700:4700::1111%eth0]/hook",
        "https://merchant.example:99999/hook",
        "https://merchant.example/with space",
    ],
)
def test_invalid_and_nonpublic_literal_urls_are_rejected(value):
    with pytest.raises(ValueError):
        validate_webhook_url(value)


@pytest.mark.parametrize("addresses", [["127.0.0.1"], ["8.8.8.8", "10.0.0.1"], ["::1"]])
async def test_dns_resolving_to_private_address_is_rejected(payment, addresses):
    async def resolver(host, port):
        return addresses

    def forbidden_transport():
        raise AssertionError("must reject destination before connecting")

    sender = HttpWebhookSender(resolver=resolver, transport_factory=forbidden_transport)
    with pytest.raises(WebhookDeliveryError, match="address is not allowed"):
        await sender.send(payment)


async def test_exact_allowlisted_hostname_supports_local_demo(payment):
    requests = []

    async def private_dns(host, port):
        return ["172.20.0.4"]

    def handler(request):
        requests.append(request)
        return httpx.Response(204)

    sender = HttpWebhookSender(
        allowed_hosts=["webhook"],
        resolver=private_dns,
        transport_factory=lambda: httpx.MockTransport(handler),
    )
    await sender.send(replace(payment, webhook_url="http://webhook:8080/hook"))
    assert str(requests[0].url) == "http://172.20.0.4:8080/hook"
    assert requests[0].headers["host"] == "webhook:8080"
    with pytest.raises(WebhookDeliveryError, match="address is not allowed"):
        await sender.send(replace(payment, webhook_url="http://webhook.attacker/hook"))


async def test_network_exception_is_sanitized(payment):
    def handler(request):
        raise httpx.ConnectError("internal failure at URL?token=private", request=request)

    sender = HttpWebhookSender(
        resolver=public_dns,
        transport_factory=lambda: httpx.MockTransport(handler),
    )
    with pytest.raises(WebhookDeliveryError) as captured:
        await sender.send(payment)
    assert str(captured.value) == "webhook transport failed (ConnectError)"


async def test_dns_failure_is_sanitized(payment):
    async def resolver(host, port):
        raise socket.gaierror("private DNS detail")

    with pytest.raises(WebhookDeliveryError, match="webhook transport failed"):
        await HttpWebhookSender(resolver=resolver).send(payment)


async def test_total_timeout_includes_dns_lookup(payment):
    async def never_finishes(host, port):
        await asyncio.Future()

    with pytest.raises(WebhookDeliveryError, match="TimeoutError"):
        await HttpWebhookSender(resolver=never_finishes, timeout=0.01).send(payment)


async def test_business_decline_is_sent_as_failed_payment(payment):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(204)

    sender = HttpWebhookSender(
        resolver=public_dns,
        transport_factory=lambda: httpx.MockTransport(handler),
    )
    await sender.send(replace(payment, status=PaymentStatus.FAILED))
    assert json.loads(requests[0].content)["status"] == "failed"


async def test_pending_payment_is_never_notified(payment):
    with pytest.raises(WebhookDeliveryError, match="unfinished payment"):
        await HttpWebhookSender().send(
            replace(payment, status=PaymentStatus.PENDING, processed_at=None)
        )
