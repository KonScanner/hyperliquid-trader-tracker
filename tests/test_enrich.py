"""Cold-start enrichment: clearinghouseState → silent book seed → correct open-vs-add."""

from decimal import Decimal
from typing import Any

from tracker.book import InMemoryBook
from tracker.config import Settings
from tracker.enrich import Enricher
from tracker.exceptions import RateLimitedError
from tracker.state import EVENT_ADD

D = Decimal


class _FakeInfoClient:
    """Stands in for HyperliquidClient — returns canned clearinghouseState per user."""

    def __init__(self, responses: dict[str, Any], fail: frozenset[str] = frozenset()) -> None:
        self._responses = responses
        self._fail = fail

    async def info(self, body: dict[str, Any]) -> Any:
        user = body.get("user")
        if user in self._fail:
            raise RateLimitedError("simulated transient failure")
        return self._responses.get(user, {"assetPositions": []})


def _chs(coin: str, szi: str, entry: str, leverage: int) -> dict[str, Any]:
    return {
        "assetPositions": [
            {
                "position": {
                    "coin": coin,
                    "szi": szi,
                    "entryPx": entry,
                    "leverage": {"type": "cross", "value": leverage},
                },
                "type": "oneWay",
            }
        ]
    }


def _enricher(client, book) -> Enricher:
    return Enricher(Settings(), book, client)


async def test_seed_wallet_installs_position_and_leverage():
    book = InMemoryBook()
    client = _FakeInfoClient({"0xa": _chs("BTC", "5", "90", 10)})
    assert await _enricher(client, book).seed_wallet("0xa") is True
    pos = book.position("0xa", "BTC")
    assert pos is not None and pos.szi == D(5)
    assert book.leverage("0xa", "BTC") == 10


async def test_seeded_then_same_direction_fill_is_add_not_open():
    book = InMemoryBook()
    client = _FakeInfoClient({"0xa": _chs("BTC", "5", "90", 10)})
    await _enricher(client, book).seed_wallet("0xa")
    from datetime import UTC, datetime

    events = book.ingest(address="0xa", coin="BTC", delta=D(1), px=D(100), ts=datetime.now(UTC))
    assert [e.kind for e in events] == [EVENT_ADD]


async def test_seed_wallet_returns_false_on_client_error():
    book = InMemoryBook()
    client = _FakeInfoClient({}, fail=frozenset({"0xa"}))
    assert await _enricher(client, book).seed_wallet("0xa") is False
    assert book.position("0xa", "BTC") is None


async def test_seed_many_partitions_seeded_and_failed():
    book = InMemoryBook()
    client = _FakeInfoClient({"0xa": _chs("BTC", "1", "90", 5)}, fail=frozenset({"0xb"}))
    seeded, failed = await _enricher(client, book).seed_many(["0xa", "0xb"])
    assert seeded == ["0xa"]
    assert failed == ["0xb"]
