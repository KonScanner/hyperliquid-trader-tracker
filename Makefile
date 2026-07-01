.PHONY: format lint typecheck test check run lock up down logs

# This machine leaks the system site-packages onto PYTHONPATH, which shadows the
# venv's typing_extensions; clear it for every recipe so the venv is authoritative.
export PYTHONPATH :=

# python-telegram-bot is a core dependency, so `uv run` installs it into the env
# by default — no `--extra` needed for `ty`/`ruff` to resolve `import telegram`
# in src/tracker/bot.py.

# ---- Checks ----
# Auto-fix imports + lint, then format (the mutating fixer).
format:
	uv run ruff check --fix .
	uv run ruff format .

# Non-mutating lint (CI shape); `make format` is the mutating fixer.
lint:
	uv run ruff check .

# Static type check with ty (https://docs.astral.sh/ty/).
typecheck:
	uv run ty check

# Offline test suite (fixtures + aiosqlite; no network/WS/Telegram).
test:
	uv run pytest -q

# Everything green before shipping: ruff + ty + pytest.
check: lint typecheck test

# ---- Run ----
# Run the tracker locally against the repo-root .env (log-only without a Telegram token).
run:
	uv run hl-tracker

# Refresh uv.lock after changing dependencies in pyproject.toml.
lock:
	uv lock

# ---- Docker ----
# Tear down any prior deployment of this project (stops + removes its
# container/network), then rebuild and start fresh in the background. Use this
# to pick up code changes — `docker compose restart` alone would NOT rebuild.
up:
	docker compose down --remove-orphans
	docker compose up --build -d

down:
	docker compose down --remove-orphans

# Follow the tracker's logs (its notifications land here in log-only mode).
logs:
	docker compose logs -f
