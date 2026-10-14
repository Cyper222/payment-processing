"""Bounded HTTP delivery with DNS pinning and stable notification identity."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import ipaddress
import json
import socket
from collections.abc import Awaitable, Callable, Collection, Sequence
from urllib.parse import urlsplit

import httpx

from payment_service.domain import Payment, PaymentStatus

Resolver = Callable[[str, int], Awaitable[Sequence[str]]]
TransportFactory = Callable[[], httpx.AsyncBaseTransport]


class WebhookDeliveryError(Exception):
    """Safe-to-log delivery failure; URLs, response bodies and secrets are omitted."""


def _hostname(value: str) -> str:
    return value.lower().rstrip(".")


def _is_public_ip(value: str) -> bool:
    address = ipaddress.ip_address(value)
    # IPv4-mapped IPv6 must obey the embedded IPv4 policy as well.
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    return address.is_global and not address.is_multicast and not address.is_reserved


def validate_webhook_url(value: str, *, allowed_hosts: Collection[str] = ()) -> str:
    """Validate syntax and literal destinations; DNS is checked again at delivery."""
    try:
        parts = urlsplit(value)
        url = httpx.URL(value)
    except (ValueError, httpx.InvalidURL) as exc:
        raise ValueError("webhook_url must be an absolute HTTP(S) URL") from exc
    if (
        url.scheme not in {"http", "https"}
        or not url.host
        or parts.username is not None
        or parts.password is not None
        or "#" in value
        or "%" in url.host
        or (url.port is not None and not 1 <= url.port <= 65535)
        or any(character.isspace() or ord(character) < 32 for character in value)
    ):
        raise ValueError("webhook_url must be HTTP(S), without credentials or a fragment")
    allowed = {_hostname(host) for host in allowed_hosts}
    host = _hostname(url.host)
    if host not in allowed:
        if host == "localhost" or host.endswith(".localhost"):
            raise ValueError("webhook_url must target a public address")
        try:
            ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            if not _is_public_ip(host):
                raise ValueError("webhook_url must target a public address")
    return str(url)


async def resolve_addresses(host: str, port: int) -> Sequence[str]:
    records = await asyncio.get_running_loop().getaddrinfo(
        host, port, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP
    )
    return tuple(dict.fromkeys(str(record[4][0]) for record in records))


class HttpWebhookSender:
    """One delivery attempt; durable retries are owned by the message handler.

    Connect to a validated literal IP, retaining the original Host header and TLS
    SNI/certificate hostname. This prevents a second DNS lookup (rebinding).
    A new transport per attempt also prevents TLS connections being shared across
    different hostnames that happen to resolve to the same IP.
    """

    def __init__(
        self,
        *,
        timeout: float = 10.0,
        allowed_hosts: Collection[str] = (),
        signing_secret: str | None = None,
        resolver: Resolver = resolve_addresses,
        transport_factory: TransportFactory | None = None,
    ) -> None:
        if timeout <= 0:
            raise ValueError("webhook timeout must be positive")
        self._timeout = timeout
        self._allowed_hosts = frozenset(_hostname(host) for host in allowed_hosts)
        self._secret = signing_secret.encode() if signing_secret else None
        self._resolver = resolver
        self._transport_factory = transport_factory

    async def send(self, payment: Payment) -> None:
        if payment.status is PaymentStatus.PENDING or payment.processed_at is None:
            raise WebhookDeliveryError("cannot notify an unfinished payment")
        body = json.dumps(
            {
                "event": "payment.completed",
                "notification_id": str(payment.id),
                "payment_id": str(payment.id),
                "status": payment.status.value,
                "amount": format(payment.amount, ".2f"),
                "currency": payment.currency.value,
                "processed_at": payment.processed_at.isoformat(),
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        try:
            async with asyncio.timeout(self._timeout):
                await self._send(payment.webhook_url, str(payment.id), body)
        except (ValueError, OSError, httpx.HTTPError, TimeoutError) as exc:
            raise WebhookDeliveryError(f"webhook transport failed ({type(exc).__name__})") from None

    async def _send(self, value: str, notification_id: str, body: bytes) -> None:
        url = httpx.URL(validate_webhook_url(value, allowed_hosts=self._allowed_hosts))
        port = url.port or (443 if url.scheme == "https" else 80)
        addresses = await self._resolver(url.host, port)
        if not addresses:
            raise WebhookDeliveryError("webhook hostname has no addresses")
        # Validate every answer, rejecting mixed public/private DNS responses.
        for address in addresses:
            if "%" in address or (
                _hostname(url.host) not in self._allowed_hosts and not _is_public_ip(address)
            ):
                raise WebhookDeliveryError("webhook address is not allowed")
            ipaddress.ip_address(address)
        pinned_url = url.copy_with(host=addresses[0])
        headers = {
            "Content-Type": "application/json",
            "Host": url.netloc.decode("ascii"),
            "Idempotency-Key": notification_id,
            "X-Webhook-Id": notification_id,
        }
        if self._secret is not None:
            signature = hmac.new(self._secret, body, hashlib.sha256).hexdigest()
            headers["X-Webhook-Signature"] = f"sha256={signature}"
        transport = (
            self._transport_factory()
            if self._transport_factory is not None
            else httpx.AsyncHTTPTransport(retries=0, trust_env=False)
        )
        async with httpx.AsyncClient(
            transport=transport,
            timeout=httpx.Timeout(self._timeout),
            follow_redirects=False,
            trust_env=False,
        ) as client:
            # Inspect only headers: an untrusted receiver cannot force buffering
            # an unlimited response body or transparent decompression bomb.
            async with client.stream(
                "POST",
                pinned_url,
                content=body,
                headers=headers,
                extensions={"sni_hostname": url.host},
            ) as response:
                if not 200 <= response.status_code < 300:
                    raise WebhookDeliveryError(f"webhook returned HTTP {response.status_code}")
