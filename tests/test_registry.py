"""Address normalization + the multi-tenant subscriber registry."""

import pytest

from tracker.registry import Registry, normalize_address

ADDR = "0x" + "ab" * 20
ADDR2 = "0x" + "cd" * 20


def test_normalize_lowercases_valid_address():
    assert normalize_address("  0x" + "AB" * 20 + " ") == ADDR


@pytest.mark.parametrize("bad", ["0x123", "not-hex", "0x" + "zz" * 20, ""])
def test_normalize_rejects_bad_address(bad):
    with pytest.raises(ValueError):
        normalize_address(bad)


def test_first_subscribe_tracks_address_others_join():
    reg = Registry()
    assert reg.is_tracked(ADDR) is False
    reg.subscribe(1, ADDR, "Alice-W")
    assert reg.is_tracked(ADDR) is True
    assert reg.addresses == frozenset({ADDR})

    reg.subscribe(2, ADDR, "Bob-W")  # same wallet, second subscriber, own label
    assert reg.subscribers(ADDR) == {1: "Alice-W", 2: "Bob-W"}
    assert len(reg) == 1  # still one tracked address


def test_unsubscribe_orphan_semantics_drive_book_cleanup():
    reg = Registry()
    reg.subscribe(1, ADDR, "A")
    reg.subscribe(2, ADDR, "B")

    assert reg.unsubscribe(1, ADDR) == (True, False)  # existed, not orphan (2 still follows)
    assert reg.is_tracked(ADDR) is True
    assert reg.unsubscribe(2, ADDR) == (True, True)  # existed, now orphan → caller drops book
    assert reg.is_tracked(ADDR) is False
    assert reg.addresses == frozenset()
    assert reg.unsubscribe(1, ADDR) == (False, False)  # already gone


def test_rename_only_affects_subscribed_chat():
    reg = Registry()
    assert reg.rename(1, ADDR, "x") is False
    reg.subscribe(1, ADDR, "old")
    reg.subscribe(2, ADDR, "other")
    assert reg.rename(1, ADDR, "new") is True
    assert reg.subscribers(ADDR) == {1: "new", 2: "other"}


def test_addresses_is_union_across_subscribers():
    reg = Registry()
    reg.subscribe(1, ADDR, "A")
    reg.subscribe(2, ADDR2, "B")
    assert reg.addresses == frozenset({ADDR, ADDR2})


def test_short_renders_compact_address():
    assert Registry.short(ADDR) == f"{ADDR[:6]}…{ADDR[-4:]}"
