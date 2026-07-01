"""One-shot helper: validate a Telegram bot token and discover your chat id.

A bot token cannot be *generated* programmatically — Telegram issues it when you create a bot
via **@BotFather** in the Telegram app (send ``/newbot``, choose a name + username, copy the
token it replies with). This helper then does the two fiddly parts for you:

1. validates the token via ``getMe`` (so you know it's live and which bot it is), and
2. prints the ``chat_id`` of everyone who has recently messaged the bot (via ``getUpdates``),

so you can paste ``TELEGRAM_BOT_TOKEN`` / ``TELEGRAM_CHAT_ID`` into your ``.env``.

Run it with ``uv run hl-tracker-telegram-setup`` (reads ``TELEGRAM_BOT_TOKEN`` from env/.env),
or pass the token explicitly: ``uv run hl-tracker-telegram-setup <token>``.
"""

import sys
from typing import Any

import httpx

from tracker.config import Settings, load_env

_API = "https://api.telegram.org"
_TIMEOUT_S = 15.0

_BOTFATHER_HELP = """\
No bot token found. To create one (one-time, in the Telegram app):

  1. Open Telegram and message @BotFather
  2. Send /newbot, then follow the prompts (bot name, then a username ending in 'bot')
  3. BotFather replies with a token like 123456789:AA...  — copy it

Then set it and re-run:

  export TELEGRAM_BOT_TOKEN=123456789:AA...
  uv run hl-tracker-telegram-setup
"""


def _resolve_token(argv: list[str]) -> str | None:
    """Token from an explicit CLI arg, else ``TELEGRAM_BOT_TOKEN`` (env or .env)."""
    if len(argv) > 1 and argv[1].strip():
        return argv[1].strip()
    load_env()
    return Settings().telegram_bot_token


def _record_chat(chat: Any, chats: dict[int, str]) -> None:
    """Record one Telegram ``chat`` object (from JSON) into ``chats`` keyed by its id."""
    if not isinstance(chat, dict):
        return
    chat_id = chat.get("id")
    if not isinstance(chat_id, int):
        return
    ctype = chat.get("type", "?")
    who = chat.get("title") or chat.get("username") or chat.get("first_name") or ctype
    chats[chat_id] = f"{who} ({ctype})"


def _discover_chats(updates: list[Any]) -> dict[int, str]:
    """Map each chat id seen in recent updates (messages / joins) to a human description."""
    chats: dict[int, str] = {}
    for update in updates:
        if not isinstance(update, dict):
            continue
        for key in ("message", "edited_message", "channel_post", "my_chat_member"):
            container = update.get(key)
            if isinstance(container, dict):
                _record_chat(container.get("chat"), chats)
    return chats


def main() -> None:
    """Console-script entry point (``hl-tracker-telegram-setup``)."""
    token = _resolve_token(sys.argv)
    if not token:
        print(_BOTFATHER_HELP)
        raise SystemExit(1)

    with httpx.Client(timeout=_TIMEOUT_S) as client:
        me = client.get(f"{_API}/bot{token}/getMe")
        payload = (
            me.json() if me.headers.get("content-type", "").startswith("application/json") else {}
        )
        if me.status_code != 200 or not payload.get("ok"):
            desc = payload.get("description", me.text[:200])
            print(f"Token rejected by Telegram (HTTP {me.status_code}): {desc}")
            raise SystemExit(1)
        bot = payload["result"]
        print(f"✓ Token valid — bot @{bot.get('username')} (id {bot.get('id')})")

        updates = client.get(f"{_API}/bot{token}/getUpdates").json().get("result", [])

    chats = _discover_chats(updates)
    if not chats:
        print(
            "\nNo chats seen yet. Send any message to your bot (or add it to a group and post "
            "there), then re-run this command to reveal your chat id."
        )
        return

    print("\nChats that have messaged this bot — copy the id you want into TELEGRAM_CHAT_ID:")
    for chat_id, who in chats.items():
        print(f"  TELEGRAM_CHAT_ID={chat_id}    # {who}")


if __name__ == "__main__":
    main()
