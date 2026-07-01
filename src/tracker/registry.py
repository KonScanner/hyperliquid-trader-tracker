"""The in-memory subscriber registry + address normalization.

Multi-tenant: each watched ``address`` maps to the set of subscribers tracking it, each with
their own label — ``{address: {chat_id: label}}``. The listener filters trades against
:attr:`Registry.addresses` (the union of all tracked wallets on one firehose connection), and on
a lifecycle event fans the notification out to every subscriber of that address.

An address is added to the filter (:meth:`subscribe`) only AFTER its cold-start seed — the caller
checks :meth:`is_tracked` first and seeds new addresses — which is what keeps an add on a
pre-existing position from being mis-reported as a new open.
"""

import re

_ADDR_RE = re.compile(r"^0x[0-9a-f]{40}$")


def normalize_address(raw: str) -> str:
    """Lowercase + validate an EVM address. Raises ``ValueError`` on a malformed input."""
    addr = raw.strip().lower()
    if not _ADDR_RE.match(addr):
        raise ValueError(f"not a valid 0x-address: {raw!r}")
    return addr


class Registry:
    """``{address: {chat_id: label}}`` with a cached ``frozenset`` of tracked addresses."""

    def __init__(self) -> None:
        self._subs: dict[str, dict[int, str]] = {}
        self._addresses: frozenset[str] = frozenset()

    def is_tracked(self, address: str) -> bool:
        """Whether any subscriber currently tracks ``address`` (i.e. it is already seeded/admitted)."""
        return address in self._subs

    def subscribe(self, chat_id: int, address: str, label: str) -> None:
        """Record ``chat_id`` as a subscriber of ``address`` (admitting it to the filter if new)."""
        subscribers = self._subs.get(address)
        if subscribers is None:
            self._subs[address] = {chat_id: label}
            self._addresses = frozenset(self._subs)
        else:
            subscribers[chat_id] = label

    def unsubscribe(self, chat_id: int, address: str) -> tuple[bool, bool]:
        """Remove one subscriber. Returns ``(existed, is_now_orphan)``.

        ``is_now_orphan`` is ``True`` when the last subscriber left, so the caller can drop the
        address from the position book and it leaves the filter.
        """
        subscribers = self._subs.get(address)
        if subscribers is None or chat_id not in subscribers:
            return False, False
        del subscribers[chat_id]
        if subscribers:
            return True, False
        del self._subs[address]
        self._addresses = frozenset(self._subs)
        return True, True

    def rename(self, chat_id: int, address: str, label: str) -> bool:
        """Relabel one subscriber's view of ``address``. Returns ``True`` if subscribed."""
        subscribers = self._subs.get(address)
        if subscribers is None or chat_id not in subscribers:
            return False
        subscribers[chat_id] = label
        return True

    def subscribers(self, address: str) -> dict[int, str]:
        """A snapshot ``{chat_id: label}`` of who tracks ``address`` (empty if none)."""
        return dict(self._subs.get(address, {}))

    @property
    def addresses(self) -> frozenset[str]:
        """The union of tracked addresses (what ``resolve_deltas`` filters against)."""
        return self._addresses

    @staticmethod
    def short(address: str) -> str:
        """A compact ``0x1234…abcd`` rendering for display/fallback."""
        return f"{address[:6]}…{address[-4:]}" if len(address) >= 10 else address

    def __len__(self) -> int:
        return len(self._subs)
