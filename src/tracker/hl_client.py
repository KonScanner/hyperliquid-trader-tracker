"""Client for Hyperliquid's official public ``/info`` API.

Hyperliquid's API is sanctioned for programmatic use — no auth, no bot block — so this is a
plain ``httpx`` client. It is the source for the per-account ``clearinghouseState`` snapshot
used to seed the in-memory book (cold-start correctness) and to refresh leverage. Ported from
the sibling ``hyperdash-crawl`` project.
"""

import asyncio
import logging
from types import TracebackType
from typing import Any, Protocol, Self, runtime_checkable

import httpx

from tracker.config import Settings
from tracker.exceptions import AuthRequiredError, ParseError, RateLimitedError
from tracker.retry import RETRYABLE_STATUS, backoff_or_raise

logger = logging.getLogger(__name__)


@runtime_checkable
class InfoClient(Protocol):
    """The one capability the enrichment layer needs from Hyperliquid."""

    async def info(self, body: dict[str, Any]) -> Any: ...


class HyperliquidClient:
    """POSTs to ``/info`` with transient-only retry/backoff."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> Self:
        self._client = httpx.AsyncClient(timeout=self._settings.http_timeout_s)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def info(self, body: dict[str, Any]) -> Any:
        """Run one ``/info`` request, returning the parsed JSON."""
        if self._client is None:
            raise RuntimeError("HyperliquidClient used outside its context manager")

        label = f"hl:{body.get('type', '?')}"
        attempt = 0
        while True:
            try:
                resp = await self._client.post(self._settings.hyperliquid_url, json=body)
            except httpx.TransportError as err:
                attempt = await backoff_or_raise(
                    attempt, self._settings, label, RateLimitedError(f"{label}: {err}")
                )
                continue

            if resp.status_code == 200:
                if self._settings.request_delay_s:
                    await asyncio.sleep(self._settings.request_delay_s)
                try:
                    return resp.json()
                except ValueError as err:
                    # A 200 with a non-JSON body (CDN interstitial, proxy error page) — convert
                    # so it stays inside our exception hierarchy.
                    raise ParseError(f"{label}: 200 body was not JSON ({resp.text[:200]})") from err
            if resp.status_code in (401, 403):
                raise AuthRequiredError(f"{label}: HTTP {resp.status_code}")
            if resp.status_code in RETRYABLE_STATUS:
                attempt = await backoff_or_raise(
                    attempt,
                    self._settings,
                    label,
                    RateLimitedError(f"{label}: HTTP {resp.status_code}"),
                )
                continue
            raise ParseError(f"{label}: HTTP {resp.status_code} ({resp.text[:200]})")
