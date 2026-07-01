"""The in-memory *admitted* watchlist and address normalization.

Distinct from :class:`tracker.db.WatchlistDB` (persistence): this is the set the listener
filters trades against. A wallet is *admitted* here only AFTER its cold-start seed completes,
which is what guarantees an add on a pre-existing position is never mis-reported as a new open.
"""

import re

_ADDR_RE = re.compile(r"^0x[0-9a-f]{40}$")


def normalize_address(raw: str) -> str:
    """Lowercase + validate an EVM address. Raises ``ValueError`` on a malformed input."""
    addr = raw.strip().lower()
    if not _ADDR_RE.match(addr):
        raise ValueError(f"not a valid 0x-address: {raw!r}")
    return addr


class Watchlist:
    """Address→label of admitted wallets, with a cached ``frozenset`` for O(1) trade filtering."""

    def __init__(self) -> None:
        self._labels: dict[str, str] = {}
        self._addresses: frozenset[str] = frozenset()

    def admit(self, address: str, label: str) -> None:
        """Add (or relabel) an admitted wallet and refresh the filter set."""
        self._labels[address] = label
        self._addresses = frozenset(self._labels)

    def forget(self, address: str) -> bool:
        """Remove a wallet from the filter. Returns ``True`` if it was present."""
        if self._labels.pop(address, None) is None:
            return False
        self._addresses = frozenset(self._labels)
        return True

    def rename(self, address: str, label: str) -> bool:
        """Relabel an admitted wallet. Returns ``True`` if it was present."""
        if address not in self._labels:
            return False
        self._labels[address] = label
        return True

    @property
    def addresses(self) -> frozenset[str]:
        """The current admitted address set (what ``resolve_deltas`` filters against)."""
        return self._addresses

    def label(self, address: str) -> str:
        """The wallet's label, or a shortened address if somehow unlabeled."""
        return self._labels.get(address) or self.short(address)

    @staticmethod
    def short(address: str) -> str:
        """A compact ``0x1234…abcd`` rendering for display/fallback."""
        return f"{address[:6]}…{address[-4:]}" if len(address) >= 10 else address

    def __contains__(self, address: str) -> bool:
        return address in self._labels

    def __len__(self) -> int:
        return len(self._labels)
