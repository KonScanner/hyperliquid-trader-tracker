"""The ADMIN_CHAT_ID / TRACKER_ALLOWED_CHAT_IDS gating set."""

import pytest

from tracker.config import Settings


def _settings(admin_chat_id: str = "", allowed_chat_ids: str = "") -> Settings:
    # Explicit init kwargs outrank the env/.env sources, keeping these tests hermetic.
    return Settings(admin_chat_id=admin_chat_id, allowed_chat_ids=allowed_chat_ids)


def test_allowed_chat_ids_set_empty_means_public():
    assert _settings().allowed_chat_ids_set == frozenset()


def test_admin_chat_id_alone_locks_to_one_chat():
    assert _settings(admin_chat_id=" 12345 ").allowed_chat_ids_set == frozenset({12345})


def test_admin_chat_id_unions_with_allowlist():
    settings = _settings(admin_chat_id="12345", allowed_chat_ids="-100200, 67890")
    assert settings.allowed_chat_ids_set == frozenset({12345, -100200, 67890})


def test_admin_chat_id_garbage_is_rejected():
    with pytest.raises(ValueError):
        _ = _settings(admin_chat_id="@my_channel").allowed_chat_ids_set
