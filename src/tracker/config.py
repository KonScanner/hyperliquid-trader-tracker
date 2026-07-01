"""Runtime configuration, sourced from a ``.env`` file or environment variables.

Most knobs use the ``TRACKER_`` prefix (e.g. ``TRACKER_WS_HEARTBEAT_S=30``). The Telegram
credentials are read un-prefixed (``TELEGRAM_BOT_TOKEN`` / ``TELEGRAM_CHAT_ID``) to match the
conventional names, and the SQLite file defaults to ``tracker.db`` at the repo root.
"""

from pathlib import Path

from dotenv import load_dotenv
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Repo root: .../src/tracker/config.py -> parents[2].
_REPO_ROOT = Path(__file__).resolve().parents[2]


def load_env() -> None:
    """Load the repo-root ``.env`` into ``os.environ`` (idempotent, CWD-agnostic).

    Existing environment variables win (``override=False``), so a container-injected value
    takes precedence over the on-disk file. A no-op when no ``.env`` exists.
    """
    load_dotenv(_REPO_ROOT / ".env", override=False)


class Settings(BaseSettings):
    """Tunable knobs for the firehose listener, enrichment sweep, and Telegram delivery."""

    model_config = SettingsConfigDict(
        env_prefix="TRACKER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    # --- Hyperliquid public endpoints (sanctioned, unauthenticated) ---
    hyperliquid_url: str = "https://api.hyperliquid.xyz/info"
    hl_ws_url: str = "wss://api.hyperliquid.xyz/ws"

    # --- HTTP client (seed + reconcile clearinghouseState calls) ---
    http_timeout_s: float = Field(default=30.0, ge=1.0)
    request_delay_s: float = Field(default=0.0, ge=0.0)  # inter-request politeness
    max_retries: int = Field(default=4, ge=0)
    backoff_base_s: float = Field(default=0.5, ge=0.0)
    backoff_cap_s: float = Field(default=20.0, ge=0.0)

    # --- WebSocket listener ---
    # HL closes any connection idle 60s, so ping quiet sockets under that threshold.
    ws_heartbeat_s: float = Field(default=30.0, ge=1.0, le=55.0)
    # Coins to subscribe to, comma-separated; empty = every perp from Hyperliquid ``meta``.
    live_coins: str = ""
    # Bounded in-memory ring of recently-seen trade ``tid`` values, so a WS reconnect that
    # redelivers trades can neither double-ingest (corrupting a position) nor double-notify.
    tid_dedup_maxlen: int = Field(default=100_000, ge=100)

    # --- Cold-start seed + reconcile (clearinghouseState, weight 2 each) ---
    # Concurrency of the startup seed sweep; bounded so 1k wallets stay within the 1200
    # weight/min IP budget (clearinghouseState = weight 2 -> <=600/min).
    seed_concurrency: int = Field(default=8, ge=1, le=50)
    # Periodically refetch clearinghouseState for recently-active wallets to refresh leverage
    # and correct any size drift from a missed WS message. 0 disables the reconcile loop.
    reconcile_interval_s: float = Field(default=45.0, ge=0.0)
    reconcile_batch: int = Field(default=20, ge=1)

    # --- Notifications ---
    # Full lifecycle by default: open/add/reduce/close/flip all push. Set False to mute the
    # exit side (reduce/close) and notify only new positions + increases.
    notify_reduce_close: bool = True

    # --- Persistence (the ONLY thing stored: per-subscriber watchlists) ---
    db_path: Path = Field(default=_REPO_ROOT / "tracker.db")

    # --- Telegram delivery ---
    # This is a MULTI-TENANT public bot: anyone can message it, subscribe to wallets, and
    # receive notifications in their own chat. The only credential is the bot token from
    # @BotFather — there is no fixed chat id (each subscriber's chat is captured on /start).
    # Optionally restrict who may subscribe to a comma-separated allowlist of chat ids.
    telegram_bot_token: str | None = Field(
        default=None,
        validation_alias=AliasChoices("TELEGRAM_BOT_TOKEN", "TRACKER_TELEGRAM_BOT_TOKEN"),
    )
    allowed_chat_ids: str = ""

    @property
    def allowed_chat_ids_set(self) -> frozenset[int]:
        """Parsed allowlist of chat ids; empty = open to anyone (public bot)."""
        return frozenset(int(c) for c in self.allowed_chat_ids.split(",") if c.strip())

    @property
    def live_coins_list(self) -> list[str]:
        """Parsed ``live_coins`` verbatim (empty list = subscribe to all perps).

        NOT upper-cased: Hyperliquid perp names are exact identifiers and some are lowercase-
        prefixed (kPEPE, kSHIB, kBONK, …), so upper-casing would silently subscribe to a feed
        that doesn't exist.
        """
        return [c.strip() for c in self.live_coins.split(",") if c.strip()]
