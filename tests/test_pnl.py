"""Authoritative close PnL: leg-window filtering, retry-until-visible, pagination, fallbacks."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import tracker.pnl as pnl_mod
from tracker.config import Settings
from tracker.exceptions import RateLimitedError
from tracker.models import CompletedTrade
from tracker.pnl import ClosedPnlResolver, with_authoritative_pnl
from tracker.state import EVENT_CLOSE, EVENT_OPEN, LiveEvent

D = Decimal
OPENED = datetime(2026, 6, 15, 12, 0, tzinfo=UTC)
CLOSED = datetime(2026, 6, 15, 15, 0, tzinfo=UTC)
OPENED_MS = round(OPENED.timestamp() * 1000)
CLOSED_MS = round(CLOSED.timestamp() * 1000)


class _FakeInfoClient:
    """Returns one canned response per call (the last repeats); records request bodies.

    An ``Exception`` instance in ``responses`` is raised instead of returned.
    """

    def __init__(self, responses: list[Any]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def info(self, body: dict[str, Any]) -> Any:
        self.calls.append(body)
        resp = self._responses.pop(0) if len(self._responses) > 1 else self._responses[0]
        if isinstance(resp, Exception):
            raise resp
        return resp


def _fill(coin: str, time_ms: int, closed_pnl: str) -> dict[str, Any]:
    return {"coin": coin, "time": time_ms, "closedPnl": closed_pnl, "px": "1", "sz": "1"}


def _trade(coin: str = "BTC") -> CompletedTrade:
    return CompletedTrade(
        address="0xa",
        coin=coin,
        direction="Long",
        start_time=OPENED,
        end_time=CLOSED,
        duration_mins=180,
    )


def _close_event(realized: str = "20") -> LiveEvent:
    return LiveEvent(
        kind=EVENT_CLOSE,
        address="0xa",
        coin="BTC",
        direction="Long",
        delta=D("-2"),
        px=D(110),
        szi_after=D(0),
        realized_pnl=D(realized),
        ts=CLOSED,
    )


def _resolver(client: _FakeInfoClient, attempts: int = 3) -> ClosedPnlResolver:
    settings = Settings(closed_pnl_attempts=attempts, closed_pnl_retry_delay_s=0.0)
    return ClosedPnlResolver(settings, client)


async def test_close_pnl_sums_closed_pnl_over_the_leg_window():
    client = _FakeInfoClient(
        [
            [
                _fill("BTC", OPENED_MS, "7.0"),  # opening fill: a flip's residue — excluded
                _fill("BTC", OPENED_MS + 1000, "5.5"),  # reduce inside the leg
                _fill("ETH", OPENED_MS + 2000, "99"),  # other coin — excluded
                _fill("BTC", CLOSED_MS, "4.5"),  # the closing fill
                _fill("BTC", CLOSED_MS + 1, "99"),  # after the close (endTime skew) — excluded
            ]
        ]
    )
    assert await _resolver(client).close_pnl(_trade(), closed_at=CLOSED) == D("10.0")


async def test_close_pnl_requests_the_leg_window():
    client = _FakeInfoClient([[_fill("BTC", CLOSED_MS, "1")]])
    await _resolver(client).close_pnl(_trade(), closed_at=CLOSED)
    body = client.calls[0]
    assert body["type"] == "userFillsByTime"
    assert body["user"] == "0xa"
    assert body["startTime"] == OPENED_MS
    assert body["endTime"] > CLOSED_MS  # skewed past the close so the closing fill can't hide


async def test_close_pnl_retries_until_the_closing_fill_is_visible():
    lagging = [_fill("BTC", OPENED_MS + 1000, "5.5")]  # REST index hasn't caught up yet
    caught_up = [*lagging, _fill("BTC", CLOSED_MS, "4.5")]
    client = _FakeInfoClient([lagging, caught_up])
    assert await _resolver(client).close_pnl(_trade(), closed_at=CLOSED) == D("10.0")
    assert len(client.calls) == 2


async def test_close_pnl_gives_up_when_the_closing_fill_never_appears():
    client = _FakeInfoClient([[_fill("BTC", OPENED_MS + 1000, "5.5")]])
    assert await _resolver(client, attempts=2).close_pnl(_trade(), closed_at=CLOSED) is None
    assert len(client.calls) == 2


async def test_close_pnl_returns_none_on_client_error():
    client = _FakeInfoClient([RateLimitedError("simulated 429")])
    assert await _resolver(client).close_pnl(_trade(), closed_at=CLOSED) is None


async def test_close_pnl_returns_none_on_unparseable_closed_pnl():
    client = _FakeInfoClient([[_fill("BTC", CLOSED_MS, "not-a-number")]])
    assert await _resolver(client).close_pnl(_trade(), closed_at=CLOSED) is None


async def test_close_pnl_returns_none_on_non_list_response():
    client = _FakeInfoClient([{"error": "nope"}])
    assert await _resolver(client).close_pnl(_trade(), closed_at=CLOSED) is None


async def test_close_pnl_paginates_full_pages(monkeypatch):
    monkeypatch.setattr(pnl_mod, "_PAGE_LIMIT", 2)
    page1 = [_fill("BTC", OPENED_MS + 1, "1"), _fill("BTC", OPENED_MS + 2, "2")]
    page2 = [_fill("BTC", CLOSED_MS, "3")]
    client = _FakeInfoClient([page1, page2])
    assert await _resolver(client).close_pnl(_trade(), closed_at=CLOSED) == D(6)
    # The walk resumes strictly past the last row of the full page.
    assert client.calls[1]["startTime"] == OPENED_MS + 3


async def test_close_pnl_gives_up_when_the_leg_overflows_the_page_cap(monkeypatch):
    monkeypatch.setattr(pnl_mod, "_PAGE_LIMIT", 1)
    monkeypatch.setattr(pnl_mod, "_MAX_PAGES", 2)
    client = _FakeInfoClient([[_fill("BTC", OPENED_MS + 1, "1")]])  # every page comes back full
    assert await _resolver(client).close_pnl(_trade(), closed_at=CLOSED) is None
    assert len(client.calls) == 2  # stopped at the page cap, not the retry budget


async def test_with_authoritative_pnl_swaps_in_the_exchange_number():
    client = _FakeInfoClient([[_fill("BTC", CLOSED_MS, "19.25")]])
    event = await with_authoritative_pnl(_close_event("20"), _trade(), _resolver(client))
    assert event.realized_pnl == D("19.25")


async def test_with_authoritative_pnl_keeps_the_estimate_when_lookup_fails():
    client = _FakeInfoClient([RateLimitedError("simulated 429")])
    event = await with_authoritative_pnl(_close_event("20"), _trade(), _resolver(client))
    assert event.realized_pnl == D(20)


async def test_with_authoritative_pnl_ignores_non_close_events():
    open_event = LiveEvent(
        kind=EVENT_OPEN,
        address="0xa",
        coin="BTC",
        direction="Long",
        delta=D(2),
        px=D(100),
        szi_after=D(2),
        realized_pnl=None,
        ts=OPENED,
    )
    client = _FakeInfoClient([[]])
    assert await with_authoritative_pnl(open_event, _trade(), _resolver(client)) is open_event
    assert client.calls == []  # no lookup spent on an open


async def test_with_authoritative_pnl_is_a_passthrough_without_a_resolver():
    event = _close_event()
    assert await with_authoritative_pnl(event, _trade(), None) is event
