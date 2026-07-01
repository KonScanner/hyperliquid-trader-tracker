"""Notification formatting + multi-tenant fan-out + the reduce/close mute toggle."""

from datetime import UTC, datetime
from decimal import Decimal

from tracker.notifier import Notifier, format_event
from tracker.state import (
    EVENT_ADD,
    EVENT_CLOSE,
    EVENT_OPEN,
    EVENT_REDUCE,
    LiveEvent,
)

D = Decimal
TS = datetime(2026, 6, 15, 12, 0, tzinfo=UTC)


def _event(kind, *, delta, szi_after="0", realized=None, direction="Long"):
    return LiveEvent(
        kind=kind,
        address="0xa",
        coin="BTC",
        direction=direction,
        delta=D(delta),
        px=D("63120"),
        szi_after=D(szi_after),
        realized_pnl=None if realized is None else D(realized),
        ts=TS,
    )


class _FakeSender:
    def __init__(self):
        self.sent: list[tuple[int, str]] = []

    async def send(self, chat_id: int, text: str) -> None:
        self.sent.append((chat_id, text))


# --- format_event (pure) --------------------------------------------------------------------


def test_open_format_leads_with_label_and_shows_leverage():
    text = format_event(_event(EVENT_OPEN, delta="2.5"), label="Whale-1", leverage=10, mark=None)
    assert text == "🟢 Started trade Whale-1: BTC Long 2.5 @ 63120 (10x)"


def test_add_format_includes_notional_from_mark():
    text = format_event(
        _event(EVENT_ADD, delta="1", szi_after="3"), label="Whale-1", leverage=10, mark=D("63120")
    )
    assert text == "➕ Added to position Whale-1: BTC Long +1 (~$63,120.00) @ 63120 (10x)"


def test_add_format_without_mark_omits_notional():
    text = format_event(_event(EVENT_ADD, delta="1"), label="W", leverage=None, mark=None)
    assert text == "➕ Added to position W: BTC Long +1 @ 63120 (?x)"


def test_reduce_format_shows_realized_and_remaining():
    text = format_event(
        _event(EVENT_REDUCE, delta="-0.5", szi_after="2", realized="440"),
        label="Whale-1",
        leverage=10,
        mark=None,
    )
    assert text == "➖ Reduced Whale-1: BTC Long -0.5 @ 63120 | realized +$440.00 | 2 left"


def test_close_format_shows_negative_pnl():
    text = format_event(
        _event(EVENT_CLOSE, delta="-2", realized="-1250.5"), label="W", leverage=10, mark=None
    )
    assert text == "🔴 Closed W: BTC Long 2 @ 63120 | realized -$1,250.50"


# --- Notifier dispatch (fan-out) ------------------------------------------------------------


async def test_dispatch_fans_out_to_each_subscriber_with_their_own_label():
    sender = _FakeSender()
    notifier = Notifier(sender, notify_reduce_close=True)
    await notifier.dispatch(
        _event(EVENT_OPEN, delta="2"),
        recipients={1: "Alice-W", 2: "Bob-W"},
        leverage=10,
        mark=None,
    )
    by_chat = dict(sender.sent)
    assert set(by_chat) == {1, 2}
    assert "Alice-W" in by_chat[1]
    assert "Bob-W" in by_chat[2]


async def test_dispatch_mutes_reduce_close_when_disabled():
    sender = _FakeSender()
    notifier = Notifier(sender, notify_reduce_close=False)
    await notifier.dispatch(
        _event(EVENT_CLOSE, delta="-2", realized="5"), recipients={1: "W"}, leverage=1, mark=None
    )
    await notifier.dispatch(
        _event(EVENT_OPEN, delta="2"), recipients={1: "W"}, leverage=1, mark=None
    )
    assert len(sender.sent) == 1
    assert sender.sent[0][1].startswith("🟢 Started trade")


async def test_dispatch_swallows_sender_failure_and_continues_to_next_recipient():
    class _FlakySender:
        def __init__(self):
            self.ok: list[int] = []

        async def send(self, chat_id: int, text: str) -> None:
            if chat_id == 1:
                raise RuntimeError("telegram down")
            self.ok.append(chat_id)

    sender = _FlakySender()
    notifier = Notifier(sender, notify_reduce_close=True)
    # Chat 1 fails, chat 2 must still receive it — best-effort delivery.
    await notifier.dispatch(
        _event(EVENT_OPEN, delta="2"), recipients={1: "A", 2: "B"}, leverage=1, mark=None
    )
    assert sender.ok == [2]
