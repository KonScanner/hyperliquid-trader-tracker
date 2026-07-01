"""Address normalization + the in-memory admitted watchlist."""

import pytest

from tracker.watchlist import Watchlist, normalize_address

ADDR = "0x" + "ab" * 20  # 42-char lowercase hex


def test_normalize_lowercases_valid_address():
    assert normalize_address("  0x" + "AB" * 20 + " ") == ADDR


@pytest.mark.parametrize("bad", ["0x123", "not-hex", "0x" + "zz" * 20, ""])
def test_normalize_rejects_bad_address(bad):
    with pytest.raises(ValueError):
        normalize_address(bad)


def test_admit_forget_and_addresses_snapshot():
    wl = Watchlist()
    wl.admit(ADDR, "Whale-1")
    assert ADDR in wl
    assert wl.addresses == frozenset({ADDR})
    assert wl.label(ADDR) == "Whale-1"
    assert wl.forget(ADDR) is True
    assert wl.forget(ADDR) is False
    assert wl.addresses == frozenset()


def test_rename_only_affects_admitted():
    wl = Watchlist()
    assert wl.rename(ADDR, "X") is False
    wl.admit(ADDR, "old")
    assert wl.rename(ADDR, "new") is True
    assert wl.label(ADDR) == "new"


def test_label_falls_back_to_short_address():
    wl = Watchlist()
    assert wl.label(ADDR) == f"{ADDR[:6]}…{ADDR[-4:]}"
